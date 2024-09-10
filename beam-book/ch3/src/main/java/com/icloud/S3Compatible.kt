package com.icloud


import com.icloud.extensions.kVia
import com.icloud.extensions.minutes
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.aws2.options.AwsOptions
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.TypeDescriptors
import org.apache.kafka.common.serialization.StringDeserializer

object S3Compatible {

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) =
            PipelineUtils.from(args, AwsOptions::class.java)

        pipeline.apply(
            KafkaIO.read<String, String>()
                .withTopic("investing-korean")
                .withBootstrapServers("node03.ming.com:6667")
                .withKeyDeserializer(StringDeserializer::class.java)
                .withValueDeserializer(StringDeserializer::class.java)
        )
            .apply(MapElements.into(TypeDescriptors.strings()).kVia { it.kv.value })
            .apply(Window.into(FixedWindows.of(1.minutes())))
            .apply(
                TextIO.write()
                    .withWindowedWrites()
                    .to("s3://test-ming-bucket/investing-korean.txt")
            )

        pipeline.run().waitUntilFinish()
    }
}