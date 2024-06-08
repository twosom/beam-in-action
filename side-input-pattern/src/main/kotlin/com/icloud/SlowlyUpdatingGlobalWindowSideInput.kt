package com.icloud

import com.icloud.extensions.kv
import com.icloud.extensions.parDo
import com.icloud.extensions.repeatedlyForever
import com.icloud.service.PlaceholderExternalService
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.Latest
import org.apache.beam.sdk.transforms.Sum
import org.apache.beam.sdk.transforms.View
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollectionView
import org.joda.time.Duration
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat
import org.slf4j.Logger
import org.slf4j.LoggerFactory

object SlowlyUpdatingGlobalWindowSideInput {

    private val LOG: Logger = LoggerFactory.getLogger(SlowlyUpdatingGlobalWindowSideInput::class.java)

    @JvmStatic
    fun main(args: Array<String>) {
        val pipeline = PipelineUtils.create<PipelineOptions>(args)
        val kvIterable = getSideInput(pipeline)

        pipeline.apply(
            "From Generate Sequence 2",
            GenerateSequence.from(0).withRate(1, Duration.standardSeconds(1L))
        )
            .apply("Windowing", Window.into(FixedWindows.of(Duration.standardSeconds(1L))))
            .apply(Sum.longsGlobally().withoutDefaults())
            .apply(
                object : DoFn<Long, KV<Long, Long>>() {
                    @ProcessElement
                    fun process(
                        c: ProcessContext,
                        @Timestamp timestamp: Instant,
                    ) = run {
                        val sideInput = c.sideInput(kvIterable)
                        val sideInputAsList = sideInput.toList()
                        assert(sideInputAsList.size == 1) { "Invalid Side Input" }
                        val kv = sideInputAsList[0]
                        c.outputWithTimestamp(1L kv c.element(), Instant.now())
                        LOG.info(
                            "Value is {} with timestamp {}, using key A from side input with time {}.",
                            c.element(),
                            timestamp.toString(DateTimeFormat.forPattern("HH:mm:ss")),
                            kv
                        )
                    }
                }.parDo().withSideInputs(kvIterable)
            )
            .apply(LogUtils.of())

        pipeline.run()
    }

    private fun getSideInput(pipeline: Pipeline): PCollectionView<MutableIterable<Map<String, String>>> =
        pipeline.apply(
            "From Generated Sequence 1",
            GenerateSequence.from(0).withRate(1, Duration.standardSeconds(5L))
        )
            .apply(
                "Convert To PlaceHolder",
                object : DoFn<Long, Map<String, String>>() {

                    @ProcessElement
                    fun process(
                        @Timestamp timestamp: Instant,
                        output: OutputReceiver<Map<String, String>>,
                    ) = run {
                        val result = PlaceholderExternalService.readTestData(timestamp)
                        output.output(result as Map<String, String>)
                    }
                }.parDo()
            )
            .apply(
                "Global Windowing And Triggering",
                Window.into<Map<String, String>>(GlobalWindows())
                    .triggering(
                        AfterProcessingTime.pastFirstElementInPane()
                            .repeatedlyForever()
                    )
                    .discardingFiredPanes()
            )
            .apply(Latest.globally())
            .apply(View.asIterable())

}