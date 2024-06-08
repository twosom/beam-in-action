package com.icloud

import com.icloud.extensions.kv
import com.icloud.extensions.parDo
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PeriodicImpulse
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.joda.time.Instant

object SlowlyUpdatingSideInputWindowing {
    @JvmStatic
    fun main(args: Array<String>) {
        val pipeline = PipelineUtils.create<PipelineOptions>(args)

        val sideInput = getSideInput(pipeline)

        val mainInput = pipeline.apply(
            "Main Input Impulse",
            PeriodicImpulse.create()
                .withInterval(Duration.standardSeconds(10L))
                .applyWindowing()
        )

        mainInput.apply(
            "Generate Output With SideInput",
            object : DoFn<Instant, KV<String, Long>>() {
                @ProcessElement
                fun process(
                    context: ProcessContext,
                ) = run {
                    val sideInputValue = context.sideInput(sideInput)
                    sideInputValue.forEach { context.output(it.key kv it.value) }
                }
            }.parDo().withSideInputs(sideInput)
        )
            .apply(LogUtils.of())


        pipeline.run()
    }

    private fun getSideInput(pipeline: Pipeline) = pipeline.apply(
        "Side Input Impulse",
        PeriodicImpulse.create()
            .withInterval(Duration.standardSeconds(5L))
            .applyWindowing()
    )
        .apply(
            "FileToRead",
            object : DoFn<Instant, String>() {
                @ProcessElement
                fun process(
                    output: OutputReceiver<String>,
                ) = run {
                    output.output("test.txt")
                }
            }.parDo()
        )
        .apply(FileIO.matchAll())
        .apply(FileIO.readMatches())
        .apply(TextIO.readFiles())
        .apply(
            object : DoFn<String, String>() {
                @ProcessElement
                fun process(
                    @Element src: String,
                    output: OutputReceiver<String>,
                ) = run {
                    output.output(src)
                }
            }.parDo()
        )
        .apply(Tokenize.of())
        .apply(Count.perElement())
        .apply(View.asMap())
}