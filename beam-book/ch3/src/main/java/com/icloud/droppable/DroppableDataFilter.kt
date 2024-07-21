package com.icloud.droppable

import com.google.common.annotations.VisibleForTesting
import com.icloud.PipelineUtils
import com.icloud.extensions.*
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.state.*
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.Reify
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.*
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.joda.time.Duration
import org.joda.time.Instant
import org.slf4j.Logger

object DroppableDataFilter :
    AbstractDroppableDataFilter() {

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, Options::class.java)

        val input = pipeline.readInput(options)
            .apply(
                "To 10 Minutes Fixed Windows",
                Window.into<String>(FixedWindows.of(10.minutes()))
                    .withAllowedLateness(30.seconds())
                    .discardingFiredPanes()
            )

        val outputs = splitDroppable(input)
        storeResult(
            result = outputs.get(mainOutput),
            bootstrapServer = options.bootstrapServer,
            topic = options.outputTopic
        )
        storeResult(
            result = outputs.get(droppableOutput),
            bootstrapServer = options.bootstrapServer,
            topic = options.outputTopicDroppable
        )

        pipeline.run()
    }

    @VisibleForTesting
    fun splitDroppable(
        input: PCollection<String>,
    ): PCollectionTuple {
        @Suppress("UNCHECKED_CAST")
        val windowingStrategy: WindowingStrategy<String, BoundedWindow> =
            input.windowingStrategy as WindowingStrategy<String, BoundedWindow>

        val windowCoder: Coder<BoundedWindow> =
            windowingStrategy.windowFn.windowCoder()

        val result = input.apply(
            "Attach Window Info",
            Reify.windows()
        )
            .apply(
                "Convert to KV",
                MapElements.into(
                    BoundedWindow::class.typeDescriptor() kvs strings()
                ).kVia { it.window kv it.value }
            )
            .setCoder(KvCoder.of(windowCoder, StringUtf8Coder.of()))
            .apply(
                "To Global Windows",
                Window.into(GlobalWindows())
            )
            .apply(
                "Apply Split Droppable Data Fn",
                SplitDroppableDataFn(
                    allowedLateness = windowingStrategy.allowedLateness,
                    mainOutput = mainOutput,
                    droppableOutput = droppableOutput
                ).parDo()
                    .withOutputTags(mainOutput, TupleTagList.of(droppableOutput))
            )


        return PCollectionTuple.of(mainOutput, rewindow(result.get(mainOutput), windowingStrategy))
            .and(droppableOutput, result.get(droppableOutput))
    }

    @Suppress("UNUSED")
    class SplitDroppableDataFn(
        private val allowedLateness: Duration,
        private val mainOutput: TupleTag<String>,
        private val droppableOutput: TupleTag<String>,
    ) : DoFn<KV<BoundedWindow, String>, String>() {

        companion object {
            private val LOG: Logger = SplitDroppableDataFn::class.logger()
        }

        @StateId("too_late")
        private val tooLateSpec: StateSpec<ValueState<Boolean>> =
            StateSpecs.value()

        @TimerId("window_gc_timer")
        private val windowGcTimerSpec: TimerSpec =
            TimerSpecs.timer(TimeDomain.EVENT_TIME)

        @ProcessElement
        fun process(
            @Element element: KV<BoundedWindow, String>,
            @StateId("too_late") tooLateState: ValueState<Boolean>,
            @TimerId("window_gc_timer") windowGcTimer: Timer,
            output: MultiOutputReceiver,
        ) {
            LOG.info("[@ProcessElement] element = {}", element)
            val isTooLateForWindow = tooLateState.read() ?: false
            if (!isTooLateForWindow) {
                val timerTimestamp = element.key.maxTimestamp() + this.allowedLateness
                if (timerTimestamp.isBefore(GlobalWindow.INSTANCE.maxTimestamp())) {
                    windowGcTimer.set(timerTimestamp)
                        .also {
                            LOG.info(
                                "[@ProcessElement] key {} window_gc_timer set at {}",
                                element.key,
                                Instant.now()
                            )
                        }
                }
            }

            if (isTooLateForWindow) output.get(this.droppableOutput).output(element.value)
            else output.get(this.mainOutput).output(element.value)
        }

        @OnTimer("window_gc_timer")
        fun onWindowGcTimer(
            @StateId("too_late") tooLateState: ValueState<Boolean>,
        ) {
            LOG.info("[@OnTimer] window_gc_timer triggered at ${Instant.now()}")
            tooLateState.write(true)
        }
    }
}