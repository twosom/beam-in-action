package com.icloud;

import static com.icloud.OptionUtils.createOption;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

import java.security.SecureRandom;
import java.util.*;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler.BadRecordErrorHandler;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaStreaming {
    private static final long MESSAGE_COUNT = 100;

    private static final int WINDOW_TIME = 30;

    private static final String[] NAMES = {"Alice", "Bob", "Charlie", "David"};

    private static final int MAX_SCORE = 100;

    private static final String TOPIC_NAME = "my-topic";

    private static final int TIME_OUTPUT_AFTER_FIRST_ELEMENT = 10;

    private static final int ALLOWED_LATENESS_TIME = 1;

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormat.forPattern("HH:mm:ss");

    public static void main(String[] args) {
        final Duration windowSize = Duration.standardSeconds(WINDOW_TIME);

        final Instant nextWindowStart =
                new Instant(
                        Instant.now().getMillis() +
                        windowSize.getMillis() -
                        Instant.now().plus(windowSize).getMillis() % windowSize.getMillis()
                );


        final Timer timer = new Timer();
        final KafkaStreamingOptions options =
                createOption(args, KafkaStreamingOptions.class);

        final KafkaProducer kafkaProducer = new KafkaProducer(options);
        timer.schedule(kafkaProducer, nextWindowStart.toDate());

        final KafkaConsumer consumer = new KafkaConsumer(options);
        consumer.run();
    }


    public interface KafkaStreamingOptions extends PipelineOptions {

        @Description("Kafka Server host")
        @Validation.Required
        String getKafkaHost();

        void setKafkaHost(String value);
    }

    static class KafkaConsumer {

        private final KafkaStreamingOptions options;

        KafkaConsumer(KafkaStreamingOptions options) {
            this.options = options;
        }

        public void run() {
            final Pipeline pipeline = Pipeline.create(options);


            final Map<String, Object> consumerConfig = new HashMap<String, Object>() {{
                put(AUTO_OFFSET_RESET_CONFIG, "latest");
            }};

            PCollection<KV<String, Integer>> pCollection;
            try (BadRecordErrorHandler<?> errorHandler =
                         pipeline.registerBadRecordErrorHandler(new LogErrors())) {
                pCollection = pipeline.apply(
                        KafkaIO.<String, Integer>read()
                                .withBootstrapServers(options.getKafkaHost())
                                .withTopic(TOPIC_NAME)
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(IntegerDeserializer.class)
                                .withConsumerConfigUpdates(consumerConfig)
                                .withBadRecordErrorHandler(errorHandler)
                                .withoutMetadata()
                );
            }

            pCollection
                    .apply(Window.<KV<String, Integer>>into(FixedWindows.of(Duration.standardSeconds(WINDOW_TIME)))
                            .triggering(Repeatedly.forever(
                                    AfterProcessingTime.pastFirstElementInPane()
                                            .plusDelayOf(Duration.standardSeconds(TIME_OUTPUT_AFTER_FIRST_ELEMENT))
                            ))
                            .withAllowedLateness(Duration.standardSeconds(ALLOWED_LATENESS_TIME))
                            .accumulatingFiredPanes()
                    )
                    .apply(Sum.integersPerKey())
                    .apply(Combine.globally(new WindowCombineFn()).withoutDefaults())
                    .apply(ParDo.of(new LogResults()));

            pipeline.run().waitUntilFinish();
            System.out.println("Pipeline Finished...");
        }

        @SuppressWarnings("unused")
        static class LogResults
                extends DoFn<Map<String, Integer>, Map<String, Integer>> {

            private static final Logger LOG =
                    LoggerFactory.getLogger(LogResults.class);

            @ProcessElement
            public void process(
                    IntervalWindow window,
                    OutputReceiver<Map<String, Integer>> output,
                    @Element Map<String, Integer> element,
                    PaneInfo pane
            ) {
                if (element == null) {
                    output.output(null);
                    return;
                }

                final String startTime = window.start().toString(dateTimeFormatter);
                final String endTime = window.end().toString(dateTimeFormatter);

                final PaneInfo.Timing timing = pane.getTiming();

                switch (timing) {
                    case EARLY:
                        LOG.info("Live score (running sum) for current round:");
                        break;
                    case ON_TIME:
                        LOG.info("Final score for the current round:");
                        break;
                    case LATE:
                        LOG.info("Late score for the round from {} to {}", startTime, endTime);
                        break;
                    default:
                        throw new RuntimeException("Unknown timing value");
                }

                for (Map.Entry<String, Integer> kv : element.entrySet()) {
                    LOG.info(String.format("%10s: %-10s%n", kv.getKey(), kv.getValue()));
                }

                if (timing == PaneInfo.Timing.ON_TIME) {
                    LOG.info("======= End of round from {} to {} =======%n%n", startTime, endTime);
                }

                output.output(element);
            }
        }

        static class WindowCombineFn
                extends Combine.CombineFn<KV<String, Integer>, Map<String, Integer>, Map<String, Integer>> {

            @Override
            public Map<String, Integer> createAccumulator() {
                return new HashMap<>();
            }

            @Override
            public Map<String, Integer> addInput(
                    Map<String, Integer> accum,
                    KV<String, Integer> input
            ) {
                assert input != null && accum != null;
                accum.merge(input.getKey(), input.getValue(), Integer::sum);
                return accum;
            }

            @Override
            public Map<String, Integer> mergeAccumulators(
                    Iterable<Map<String, Integer>> accums
            ) {
                final Map<String, Integer> result = new HashMap<>();

                for (Map<String, Integer> accum : accums) {
                    for (Map.Entry<String, Integer> kv : accum.entrySet()) {
                        result.merge(kv.getKey(), kv.getValue(), Integer::sum);
                    }
                }

                return result;
            }

            @Override
            public Map<String, Integer> extractOutput(
                    Map<String, Integer> accum) {
                return accum;
            }
        }
    }


    static class KafkaProducer
            extends TimerTask {

        private final KafkaStreamingOptions options;

        KafkaProducer(KafkaStreamingOptions options) {
            this.options = options;
        }

        @Override
        public void run() {
            final Pipeline pipeline = Pipeline.create(options);

            final PCollection<KV<String, Integer>> input = pipeline.apply(
                            GenerateSequence.from(0)
                                    .withRate(MESSAGE_COUNT, Duration.standardSeconds(WINDOW_TIME))
                                    .withTimestampFn(e -> new Instant(System.currentTimeMillis()))
                    )
                    .apply(ParDo.of(new RandomUserScoreGenFn()));

            input.apply(
                    KafkaIO.<String, Integer>write()
                            .withBootstrapServers(options.getKafkaHost())
                            .withTopic(TOPIC_NAME)
                            .withKeySerializer(StringSerializer.class)
                            .withValueSerializer(IntegerSerializer.class)
                            .withProducerConfigUpdates(new HashMap<String, Object>() {{
                                // partition with randomly
                                put("partitioner.class", RoundRobinPartitioner.class.getName());
                            }})
            );

            pipeline.run()
                    .waitUntilFinish();
        }

        @SuppressWarnings("unused")
        static class RandomUserScoreGenFn
                extends DoFn<Long, KV<String, Integer>> {
            private transient Random random;

            @Setup
            public void setup() {
                this.random = new SecureRandom();
            }

            @ProcessElement
            public void process(OutputReceiver<KV<String, Integer>> output) {
                output.output(this.generate());
            }

            private KV<String, Integer> generate() {
                final String name = NAMES[this.random.nextInt(NAMES.length)];
                final int randomScore = this.random.nextInt(MAX_SCORE);
                return KV.of(name, randomScore);
            }

        }
    }

    static class LogErrors
            extends PTransform<@NonNull PCollection<BadRecord>, @NonNull PCollection<BadRecord>> {

        @Override
        public PCollection<BadRecord> expand(PCollection<BadRecord> input) {
            return input.apply(ParDo.of(new LogErrorFn()));
        }

        static class LogErrorFn
                extends DoFn<BadRecord, BadRecord> {

            private final Counter badRecordCounter =
                    Metrics.counter(getClass(), "bad record counter");

            @ProcessElement
            public void process(
                    @Element BadRecord record,
                    OutputReceiver<BadRecord> output
            ) {
                this.badRecordCounter.inc();
                System.out.println(record);
                output.output(record);
            }
        }
    }


}
