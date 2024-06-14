package com.icloud;

import com.google.common.annotations.VisibleForTesting;
import com.icloud.watermark.policy.PreventIdleWatermarkPolicy;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.io.Serializable;
import java.util.Comparator;

import static org.apache.beam.sdk.values.TypeDescriptors.kvs;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class MaxWordLength {
    public static void main(String[] args) {
        final Pipeline pipeline = PipelineUtils.create(args, CommonKafkaOptions.class);
        final CommonKafkaOptions options = pipeline.getOptions().as(CommonKafkaOptions.class);

        final PCollection<String> lines = pipeline.apply(
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
                .apply(Values.create());

        final PCollection<TimestampedValue<String>> output = computeLongestWord(lines);

        output.apply(
                "Convert To KV",
                        MapElements.into(kvs(strings(), strings()))
                                .via(kv -> KV.of(kv.getTimestamp().toString(), kv.getValue()))
                )
                .apply(
                        KafkaIO.<String, String>write()
                                .withBootstrapServers(options.getBootstrapServer())
                                .withKeySerializer(StringSerializer.class)
                                .withValueSerializer(StringSerializer.class)
                                .withTopic(options.getOutputTopic())
                );

        pipeline.run();
    }

    @VisibleForTesting
    static PCollection<TimestampedValue<String>> computeLongestWord(PCollection<String> lines) {
        return lines.apply(Tokenize.of())
                .apply(
                        "Global Windowing & Triggering",
                        Window.<String>into(new GlobalWindows())
                                .triggering(
                                        Repeatedly.forever(AfterPane.elementCountAtLeast(1))
                                )
                                .withAllowedLateness(Duration.ZERO)
                                .withTimestampCombiner(TimestampCombiner.LATEST)
                                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY)
                                .accumulatingFiredPanes()
                )
                .apply(Max.globally(
                        (Comparator<String> & Serializable)
                                (a, b) -> Long.compare(a.length(), b.length())
                ))
                .apply(Reify.timestamps())
                ;
    }
}
