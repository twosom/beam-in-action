package com.icloud

import com.google.common.annotations.VisibleForTesting
import com.icloud.coder.MetricCoder
import com.icloud.coder.PositionCoder
import com.icloud.extensions.kVia
import com.icloud.extensions.repeatedlyForever
import com.icloud.model.Metric
import com.icloud.model.Position
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory
import org.apache.beam.sdk.transforms.FlatMapElements
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors.kvs
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import org.joda.time.Duration
import org.joda.time.Instant
import java.util.*
import javax.annotation.Nullable

object SportTracker {
    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) =
            PipelineUtils.from(args, CommonKafkaOptions::class.java)
        registerCoders(pipeline)

        val records = pipeline
            .apply(
                KafkaIO.read<String, String>()
                    .withBootstrapServers(options.bootstrapServer)
                    .withKeyDeserializer(StringDeserializer::class.java)
                    .withValueDeserializer(StringDeserializer::class.java)
                    .withTopic(options.inputTopic)
                    .withTimestampPolicyFactory(newTimestampPolicy())
                    .withoutMetadata()
            )
            .apply(
                FlatMapElements.into(kvs(strings(), TypeDescriptor.of(Position::class.java)))
                    .kVia {
                        it.value.split("\t", limit = 2)
                            .takeIf { kv -> kv.size > 1 }.orEmpty()
                            .let { kv -> kv to Position.parseFrom(kv[1]) }
                            .let { pair -> listOf(KV.of(pair.first[0], pair.second)) }
                    }
            )

        val metrics = this.computeTrackMetrics(records)

        metrics.apply(
            MapElements.into(
                kvs(strings(), strings())
            )
                .kVia {
                    KV.of(
                        "",
                        "${it.key}\t${it.value.length}\t${if (it.value.duration > 0) it.value.duration * 1000 / (60.0 * it.value.length) else 0}"
                    )
                }
        )
            .apply(
                KafkaIO.write<String, String>()
                    .withBootstrapServers(options.bootstrapServer)
                    .withTopic(options.outputTopic)
                    .withKeySerializer(StringSerializer::class.java)
                    .withValueSerializer(StringSerializer::class.java)
            )

        pipeline.run().waitUntilFinish()
    }

    @VisibleForTesting
    fun registerCoders(
        pipeline: Pipeline,
    ) =
        with(pipeline.coderRegistry) {
            registerCoderForClass(Position::class.java, PositionCoder.of())
            registerCoderForClass(Metric::class.java, MetricCoder.of())
        }

    @VisibleForTesting
    fun computeTrackMetrics(
        records: PCollection<KV<String, Position>>,
    ): PCollection<KV<String, Metric>> = records.apply(
        "globalWindow",
        Window.into<KV<String, Position>>(GlobalWindows())
            .withTimestampCombiner(TimestampCombiner.LATEST)
            .withAllowedLateness(Duration.standardHours(1L))
            .triggering(
                AfterProcessingTime.pastFirstElementInPane()
                    .plusDelayOf(Duration.standardSeconds(10))
                    .repeatedlyForever()
            )
            .accumulatingFiredPanes()
    )
        .apply("groupByWorkoutId", GroupByKey.create())
        .apply(
            FlatMapElements.into(kvs(strings(), TypeDescriptor.of(Metric::class.java)))
                .kVia { computeRawMetrics(it) }
        )

    private fun computeRawMetrics(
        stringIterableKV: KV<String, Iterable<Position>>,
    ): Iterable<KV<String, Metric>> {
        val sortedPositions = stringIterableKV.value
            .sortedBy { it.timestamp }

        @Nullable
        var before: Position? = null
        var distance = .0
        var startTs: Long = Long.MIN_VALUE
        for (p in sortedPositions) {
            if (before == null) {
                startTs = p.timestamp
            } else {
                distance += before.distance(p)
            }
            before = p
        }

        val duration = before?.timestamp?.minus(startTs) ?: 0

        return Collections.singletonList(
            KV.of(stringIterableKV.key, Metric(distance, duration))
        )
    }

    private fun newTimestampPolicy(): TimestampPolicyFactory<String, String> {
        return TimestampPolicyFactory<String, String> { _, previousWatermark ->
            CustomTimestampPolicyWithLimitedDelay(
                { record ->
                    (Position.parseFrom(record.kv.value)
                        ?: return@CustomTimestampPolicyWithLimitedDelay BoundedWindow.TIMESTAMP_MIN_VALUE)
                        .let { p -> Instant.ofEpochMilli(p.timestamp) }
                },
                Duration.standardSeconds(1),
                previousWatermark
            )
        }
    }
}