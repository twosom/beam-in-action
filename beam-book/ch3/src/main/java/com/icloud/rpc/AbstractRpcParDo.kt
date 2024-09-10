package com.icloud.rpc

import com.icloud.CommonKafkaOptions
import com.icloud.MapToLines
import com.icloud.extensions.kVia
import com.icloud.extensions.kv
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors.kvs
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer


abstract class AbstractRpcParDo {
    interface DefaultOptions
        : CommonKafkaOptions {
        @get:Description("The port of for RPC Service")
        @get:Default.Integer(1234)
        var port: Int
    }

    protected fun Pipeline.readInput(options: DefaultOptions) =
        apply(
            KafkaIO.read<String, String>()
                .withBootstrapServers(options.bootstrapServer)
                .withKeyDeserializer(StringDeserializer::class.java)
                .withValueDeserializer(StringDeserializer::class.java)
                .withTopic(options.inputTopic)
        )
            .apply(MapToLines.of())

    protected fun storeResult(
        result: PCollection<KV<String, Int>>,
        options: DefaultOptions,
    ) {
        result.apply(
            MapElements.into(kvs(strings(), strings()))
                .kVia { "" kv "${it.key} ${it.value}" }
        )
            .apply(
                KafkaIO.write<String, String>()
                    .withBootstrapServers(options.bootstrapServer)
                    .withKeySerializer(StringSerializer::class.java)
                    .withValueSerializer(StringSerializer::class.java)
                    .withTopic(options.outputTopic)
            )
    }
}