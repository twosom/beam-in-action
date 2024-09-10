package com.icloud

import com.icloud.coder.PositionCoder
import com.icloud.model.Position
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.io.kafka.TimestampPolicy
import org.apache.beam.sdk.io.kafka.TimestampPolicyFactory
import org.apache.beam.sdk.transforms.FlatMapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ProcessFunction
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptor
import org.apache.beam.sdk.values.TypeDescriptors.kvs
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.apache.kafka.common.serialization.StringDeserializer
import org.joda.time.Instant
import java.util.*

class ReadPositionFromKafka(
    private val bootstrapServer: String,
    private val inputTopic: String,
) : PTransform<PBegin, PCollection<KV<String, Position>>>() {

    override fun expand(
        input: PBegin,
    ): PCollection<KV<String, Position>> =
        input.apply(
            "Read Input",
            KafkaIO.read<String, String>()
                .withBootstrapServers(this.bootstrapServer)
                .withTopic(this.inputTopic)
                .withKeyDeserializer(StringDeserializer::class.java)
                .withValueDeserializer(StringDeserializer::class.java)
                .withTimestampPolicyFactory(this.newTimestampPolicy())
        )
            .apply(
                FlatMapElements.into(kvs(strings(), TypeDescriptor.of(Position::class.java)))
                    .via(ProcessFunction { r ->
                        val kv = r.kv.value.split("\t", limit = 2)
                        if (kv.size > 1) {
                            val parsed = Position.parseFrom(kv[1])
                            if (parsed != null) {
                                return@ProcessFunction Collections.singletonList(KV.of(kv[0], parsed))
                            }
                        }
                        return@ProcessFunction Collections.emptyList()
                    })
            )
            .setCoder(KvCoder.of(StringUtf8Coder.of(), PositionCoder.of()))

    private fun newTimestampPolicy(): TimestampPolicyFactory<String, String> {
        return TimestampPolicyFactory { tp, prevWatermark ->
            object : TimestampPolicy<String, String>() {

                private var maxTimestamp: Instant = BoundedWindow.TIMESTAMP_MIN_VALUE

                override fun getTimestampForRecord(
                    ctx: PartitionContext,
                    record: KafkaRecord<String, String>,
                ): Instant {
                    val position = Position.parseFrom(record.kv.value)
                    val stamp =
                        if (position == null) BoundedWindow.TIMESTAMP_MAX_VALUE else Instant.ofEpochMilli(position.timestamp)

                    if (stamp.isAfter(this.maxTimestamp)) {
                        this.maxTimestamp = stamp
                    }
                    return stamp
                }

                override fun getWatermark(
                    ctx: PartitionContext,
                ): Instant = this.maxTimestamp - 100

            }
        }
    }

}