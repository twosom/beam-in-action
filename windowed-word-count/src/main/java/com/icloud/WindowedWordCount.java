package com.icloud;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class WindowedWordCount {

    static final int WINDOW_SIZE = 10; // Default window duration in minutes

    public static void runWindowedWordCount(Options options) throws IOException {
        final String output = options.getOutput();
        final Instant minTimestamp = new Instant(options.getMinTimestampMillis());
        final Instant maxTimestamp = new Instant(options.getMaxTimestampMillis());


        final Pipeline pipeline = Pipeline.create(options);

        final PCollection<String> input = pipeline.apply(TextIO.read().from(options.getInputFile()))
                .apply(ParDo.of(new AddTimestampFn(minTimestamp, maxTimestamp)));

        final PCollection<String> windowedWords =
                input.apply(Window.into(FixedWindows.of(Duration.standardMinutes(options.getWindowSize()))));

        final PCollection<KV<String, Long>> wordCounts =
                windowedWords.apply(CountWords.of());

        wordCounts
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply(new WriteOneFilePerWindow(output, options.getNumShards()));

        final PipelineResult result = pipeline.run();
        try {
            result.waitUntilFinish();
        } catch (Exception e) {
            result.cancel();
        }
    }

    public static void main(String[] args) throws IOException {
        final Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

        runWindowedWordCount(options);
    }

    public interface Options
            extends WordCountOptions, ExampleOptions, ExampleBigQueryTableOptions {

        @Description("Fixed window duration, in minutes")
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        @Description("Minimum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToCurrentSystemTime.class)
        Long getMinTimestampMillis();

        void setMinTimestampMillis(Long value);

        @Description("Maximum randomly assigned timestamp, in milliseconds-since-epoch")
        @Default.InstanceFactory(DefaultToMinTimestampPlusOneHour.class)
        Long getMaxTimestampMillis();

        void setMaxTimestampMillis(Long value);

        @Description("Fixed number of shards to produce per window")
        Integer getNumShards();

        void setNumShards(Integer numShards);

    }

    private static class AddTimestampFn extends DoFn<String, String> {
        private final Instant minTimestamp;
        private final Instant maxTimestamp;

        public AddTimestampFn(Instant minTimestamp, Instant maxTimestamp) {
            this.minTimestamp = minTimestamp;
            this.maxTimestamp = maxTimestamp;
        }

        @ProcessElement
        public void process(
                @Element String line,
                OutputReceiver<String> output
        ) {
            final Instant randomTimestamp = new Instant(ThreadLocalRandom.current()
                    .nextLong(minTimestamp.getMillis(), maxTimestamp.getMillis()));

            output.outputWithTimestamp(line, randomTimestamp);
        }
    }

    /**
     * A {@link DefaultValueFactory} that returns the current system time.
     */
    public static class DefaultToCurrentSystemTime implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return System.currentTimeMillis();
        }

    }

    /**
     * A {@link DefaultValueFactory} that returns the minimum timestamp plus one hour.
     */
    public static class DefaultToMinTimestampPlusOneHour implements DefaultValueFactory<Long> {
        @Override
        public Long create(PipelineOptions options) {
            return options.as(Options.class).getMinTimestampMillis()
                   + Duration.standardHours(1).getMillis();
        }

    }

    static class CountWords
            extends PTransform<@NonNull PCollection<String>, @NonNull PCollection<KV<String, Long>>> {

        static CountWords of() {
            return new CountWords();
        }

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> input) {
            return input.apply(ParDo.of(new DoFn<String, String>() {

                        private final Counter emptyLines =
                                Metrics.counter(this.getClass(), "emptyLines");

                        private final Distribution lineLenDist =
                                Metrics.distribution(this.getClass(), "lineLenDistro");

                        @ProcessElement
                        public void process(
                                @Element String element,
                                OutputReceiver<String> output
                        ) {
                            this.lineLenDist.update(element.length());
                            if (element.trim().isEmpty()) {
                                this.emptyLines.inc();
                            }

                            for (String word : element.split(ExampleUtils.TOKENIZER_PATTERN, -1)) {
                                if (!word.isEmpty()) {
                                    output.output(word);
                                }
                            }
                        }
                    }))
                    .apply(Count.perElement());
        }
    }
}
