package com.icloud

import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ProcessFunction
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.apache.beam.sdk.values.TypeDescriptors.kvs
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.apache.kafka.common.serialization.StringSerializer
import java.lang.Boolean.TRUE

class WriteNotificationsToKafka(
    private val bootstrapServer: String,
    private val outputTopic: String,
) : PTransform<PCollection<KV<String, Boolean>>, PDone>() {

    override fun expand(
        input: PCollection<KV<String, Boolean>>,
    ): PDone =
        input
            .apply(
                MapElements.into(kvs(strings(), strings()))
                    .via(ProcessFunction { KV.of("", this.formatMessage(it)) })
            )
            .apply(
                "writeOutput",
                KafkaIO.write<String, String>()
                    .withBootstrapServers(this.bootstrapServer)
                    .withTopic(this.outputTopic)
                    .withKeySerializer(StringSerializer::class.java)
                    .withValueSerializer(StringSerializer::class.java)
            )

    private fun formatMessage(
        message: KV<String, Boolean>,
    ): String {
        val user = message.key.split(":")[0]
        if (TRUE == message.value) {
            return "Great job user $user! You are gaining speed!"
        }
        return "Looks like you are slowing down user $user. Go! Go! Go!"
    }


}