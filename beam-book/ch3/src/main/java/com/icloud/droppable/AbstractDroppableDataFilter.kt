package com.icloud.droppable

import com.google.common.annotations.VisibleForTesting
import com.icloud.CommonKafkaOptions
import com.icloud.MapToLines
import com.icloud.extensions.*
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.TimestampPolicy
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.Validation
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.apache.beam.sdk.values.WindowingStrategy
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.Duration
import org.joda.time.Instant

abstract class AbstractDroppableDataFilter {

    interface Options
        : CommonKafkaOptions {
        @get:Description("The output topic for publish droppable data")
        @get:Validation.Required
        var outputTopicDroppable: String
    }

    @VisibleForTesting
    val mainOutput: TupleTag<String> =
        object : TupleTag<String>("Main Output") {}

    @VisibleForTesting
    val droppableOutput: TupleTag<String> =
        object : TupleTag<String>("Droppable Output") {}


    protected fun storeResult(
        result: PCollection<String>,
        bootstrapServer: String,
        topic: String,
    ) {
        result
            .apply(
                MapElements.into(
                    strings() kvs strings()
                ).kVia { "" kv it }
            )
            .apply(
                KafkaIO.write<String, String>()
                    .withBootstrapServers(bootstrapServer)
                    .withKeySerializer(StringSerializer::class.java)
                    .withValueSerializer(StringSerializer::class.java)
                    .withTopic(topic)
            )
    }

    protected fun <T, W : BoundedWindow> rewindow(
        input: PCollection<T>,
        windowingStrategy: WindowingStrategy<T, W>,
    ): PCollection<T> {
        val base: Window<T> = Window.into(windowingStrategy.windowFn)
            .withAllowedLateness(windowingStrategy.allowedLateness, windowingStrategy.closingBehavior)
            .withOnTimeBehavior(windowingStrategy.onTimeBehavior)
            .withTimestampCombiner(windowingStrategy.timestampCombiner)
            .triggering(windowingStrategy.trigger)

        if (windowingStrategy.mode === AccumulationMode.ACCUMULATING_FIRED_PANES) {
            return input.apply(base.accumulatingFiredPanes())
        }
        return input.apply(base.discardingFiredPanes())
    }


    protected fun Pipeline.readInput(
        options: Options,
    ): PCollection<String> = apply(
        "Read From Kafka",
        KafkaIO.read<String, String>()
            .withBootstrapServers(options.bootstrapServer)
            .withKeyDeserializer(StringDeserializer::class.java)
            .withValueDeserializer(StringDeserializer::class.java)
            .withTimestampPolicyFactory(CustomTimestampPolicyFactory())
            .withTopic(options.inputTopic)
    )
        .apply(
            "Only Values",
            MapToLines.ofValuesOnly<String, String>()
        )

    private class CustomTimestampPolicyFactory : KTimestampPolicyFactory<String, String>() {

        override fun createTimestampPolicy(
            tp: TopicPartition,
            previousWatermark: Instant?,
        ): TimestampPolicy<String, String> {
            return CustomTimestampPolicyWithLimitedDelay(
                {
                    Duration.standardMinutes(it.kv.key.toLong())
                        .let { minutes -> Instant.now() + minutes }
                },
                1.seconds(),
                previousWatermark.optional()
            )
        }


    }
}