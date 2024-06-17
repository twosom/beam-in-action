package com.icloud

import com.icloud.extensions.kv
import com.icloud.extensions.parDo
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.options.Default
import org.apache.beam.sdk.options.Description
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.options.Validation.Required
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PeriodicImpulse
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.values.KV
import org.joda.time.Duration
import org.joda.time.Instant

object SlowlyUpdatingSideInputWindowing {

    interface Options
        : PipelineOptions {

        @get:Description(
            """
                The input file path for create side input.
                This file will be read periodic.
            """
        )
        @get:Required
        var filePath: String

        @get:Description(
            """
                The interval value for lookup new files.
            """
        )
        @get:Default.Long(10L)
        var lookupInterval: Long

    }

    @JvmStatic
    fun main(args: Array<String>) {
        val pipeline = PipelineUtils.createWithHdfsConf("hdfs://node01.ming.com:8020", args, Options::class.java)
        val options = pipeline.options.`as`(Options::class.java)

        val sideInput = pipeline.getSideInput(
            options.filePath,
            options.lookupInterval
        )

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

    private fun Pipeline.getSideInput(
        filePath: String,
        lookupInterval: Long,
    ) = apply(
        "Side Input Impulse",
        PeriodicImpulse.create()
            .withInterval(Duration.standardSeconds(lookupInterval))
            .applyWindowing()
    )
        .apply(
            "FileToRead",
            object : DoFn<Instant, String>() {
                @ProcessElement
                fun process(
                    output: OutputReceiver<String>,
                ) = run {
                    output.output(filePath)
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