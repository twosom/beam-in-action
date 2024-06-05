package com.icloud;

import com.google.common.annotations.VisibleForTesting;
import com.icloud.watermark.policy.PreventIdleWatermarkPolicy;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Top;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Comparator;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class TopKWords {

    public interface TopKWordsOptions
            extends CommonKafkaOptions {

        @Validation.Required
        @Description("length of the window ::: UNIT = SECOND")
        Integer getWindowLength();

        void setWindowLength(Integer value);

        @Validation.Required
        @Description("k value for Top K")
        Integer getK();

        void setK(Integer value);

    }


    public static void main(String[] args) {
        final Pipeline pipeline = PipelineUtils.create(args, TopKWordsOptions.class);
        final TopKWordsOptions options =
                pipeline.getOptions().as(TopKWordsOptions.class);

        final PCollection<String> lines =
                pipeline.apply(
                                KafkaIO.<String, String>read()
                                        .withBootstrapServers(options.getBootstrapServer())
                                        .withKeyDeserializer(StringDeserializer.class)
                                        .withValueDeserializer(StringDeserializer.class)
                                        .withTopic(options.getInputTopic())
                                        .withTimestampPolicyFactory((tp, previousWatermark) ->
                                                PreventIdleWatermarkPolicy.of(e -> Instant.now(),
                                                        Duration.standardSeconds(1),
                                                        previousWatermark,
                                                        true
                                                ))
                                        .withoutMetadata()
                        )
                        .apply(MapElements.into(strings()).via(KV::getValue));
        final PCollection<KV<String, Long>> output =
                countWordsInFixedWindows(
                        lines,
                        Duration.standardSeconds(options.getWindowLength()), options.getK()
                );


        output.apply(
                KafkaIO.<String, Long>write()
                        .withBootstrapServers(options.getBootstrapServer())
                        .withKeySerializer(StringSerializer.class)
                        .withValueSerializer(LongSerializer.class)
                        .withTopic(options.getOutputTopic())
        );

        pipeline.run();
    }

    @VisibleForTesting
    static PCollection<KV<String, Long>> countWordsInFixedWindows(
            PCollection<String> lines,
            Duration size, Integer k
    ) {
        return lines.apply(Window.into(FixedWindows.of(size)))
                .apply(Tokenize.of())
                .apply(Count.perElement())
                .apply(Top.of(
                        k,
                        (Comparator<KV<String, Long>> & Serializable)
                                (a, b) -> Long.compare(a.getValue(), b.getValue())
                ).withoutDefaults())
                .apply(Flatten.iterables());
    }
}
