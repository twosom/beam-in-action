package com.icloud

import com.google.common.annotations.VisibleForTesting
import com.icloud.extensions.repeatedlyForever
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.CoderRegistry
import org.apache.beam.sdk.coders.CustomCoder
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.ProcessFunction
import org.apache.beam.sdk.transforms.WithKeys
import org.apache.beam.sdk.transforms.windowing.AfterPane
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.util.VarInt
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.apache.kafka.common.serialization.DoubleSerializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.serialization.StringSerializer
import java.io.InputStream
import java.io.OutputStream


object AverageWordLength {

    interface Options : CommonKafkaOptions {

        @get:Default.Boolean(true)
        var disableDefault: Boolean
    }

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) =
            PipelineUtils.from(args, Options::class.java)

        val words = pipeline.apply(
            "Read From Kafka",
            KafkaIO.read<String, String>()
                .withBootstrapServers(options.bootstrapServer)
                .withKeyDeserializer(StringDeserializer::class.java)
                .withValueDeserializer(StringDeserializer::class.java)
                .withTopic(options.inputTopic)
                .withoutMetadata()
        )
            .apply(
                MapElements.into(strings())
                    .via(ProcessFunction { it.value })
            )
            .apply(Tokenize.of())

        calculateWordLength(words, options.disableDefault)
            .apply("Key Mapping", WithKeys.of(""))
            .apply(
                "Write To Kafka",
                KafkaIO.write<String, Double>()
                    .withBootstrapServers(options.bootstrapServer)
                    .withKeySerializer(StringSerializer::class.java)
                    .withValueSerializer(DoubleSerializer::class.java)
                    .withTopic(options.outputTopic)
            )

        pipeline.run()
    }

    @JvmStatic
    @VisibleForTesting
    fun calculateWordLength(
        words: PCollection<String>,
        disableDefault: Boolean,
    ) =
        words.apply(
            Window.into<String>(GlobalWindows())
                .triggering(
                    AfterPane.elementCountAtLeast(1)
                        .repeatedlyForever()
                )
                .accumulatingFiredPanes()
                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY)
        ).apply(
            if (disableDefault) Combine.globally(AverageFn())
                .withoutDefaults()
            else Combine.globally(AverageFn())
        )


    class AverageFn :
        CombineFn<String, AverageFn.AverageAccumulator, Double>() {

        class AverageAccumulatorCoder : CustomCoder<AverageAccumulator>() {
            override fun encode(
                value: AverageAccumulator,
                outStream: OutputStream,
            ) {
                VarInt.encode(value.sumLength, outStream)
                VarInt.encode(value.count, outStream)
            }

            override fun decode(
                inStream: InputStream,
            ): AverageAccumulator =
                AverageAccumulator(
                    VarInt.decodeLong(inStream),
                    VarInt.decodeLong(inStream),
                )
        }

        data class AverageAccumulator(
            val sumLength: Long,
            val count: Long,
        ) {
            constructor() : this(0, 0)

            fun add(that: AverageAccumulator) =
                AverageAccumulator(this.sumLength + that.sumLength, this.count + that.count)

            fun add(str: String) =
                AverageAccumulator(this.sumLength + str.length, this.count + 1)
        }


        override fun createAccumulator() = AverageAccumulator()

        override fun addInput(
            accum: AverageAccumulator,
            input: String,
        ): AverageAccumulator = accum.add(input)

        override fun mergeAccumulators(
            accumulators: MutableIterable<AverageAccumulator>,
        ): AverageAccumulator =
            createAccumulator()
                .apply { println("Local Accum Created...") }
                .let { newAccum ->
                    accumulators.fold(newAccum) { acc, restAcc -> acc.add(restAcc) }
                }

        override fun extractOutput(
            accumulator: AverageAccumulator,
        ): Double = accumulator.sumLength / accumulator.count.toDouble()

        override fun getAccumulatorCoder(
            registry: CoderRegistry,
            inputCoder: Coder<String>,
        ): Coder<AverageAccumulator> = AverageAccumulatorCoder()
    }
}