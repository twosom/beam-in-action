package com.icloud.droppable

import com.google.common.annotations.VisibleForTesting
import com.icloud.LogUtils
import com.icloud.PipelineUtils
import com.icloud.extensions.*
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.InstantCoder
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.metrics.Counter
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.state.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.*
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.joda.time.Duration
import org.joda.time.Instant


object DroppableDataFilter2 : AbstractDroppableDataFilter() {

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, Options::class.java)

        val input = pipeline.readInput(options)
            .apply(
                "ReadInput__FixedWindowing__10Minutes",
                // 어차피 GlobalWindow를 적용하는데도
                // 이렇게 윈도잉을 거는 것은
                // 사용자가 이 단위로 이만큼 늦은 데이터까지는 처리하고 싶다는 것을 사전에 정의하기 위함
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

        pipeline.run().waitUntilFinish()
    }

    /**
     * if this [WindowFn] is `instanceof` [FixedWindows], and if this [FixedWindows.size] is greater than 5 seconds, limit to 5 seconds.
     * or using [FixedWindows.size]
     *
     * and if  this [WindowFn] is not `instanceof` [FixedWindows], then use just 1 second
     * @param InputT Type of input
     * @param WindowT Type of window
     *
     */
    private val <InputT, WindowT : BoundedWindow> WindowFn<InputT, WindowT>.loopDuration: Duration
        get() {
            this::class.logger().info("WindowFn type = {}", this.javaClass.simpleName)
            return when (this) {
                is FixedWindows -> if (this.size.millis > 5_000) 5.seconds() else this.size
                else -> 1.seconds()
            }
        }

    //TODO
    // we need to analyze this logic
    @VisibleForTesting
    fun splitDroppable(
        input: PCollection<String>,
    ): PCollectionTuple {
        val windowingStrategy: WindowingStrategy<String, BoundedWindow> =
            input.typeWindowingStrategy
        val windowFn = windowingStrategy.windowFn
        val windowCoder: Coder<BoundedWindow> = windowFn.windowCoder()

        //TODO
        // impulse
        val impulseString = input.pipeline
            .apply(Impulse.create())
            .apply("Convert Impulse String",
                MapElements.into(strings()).kVia { "" }
            )

        //TODO
        // watermark only
        val inputWatermarkOnly =
            input.apply(
                "InputWatermark_GlobalWindowing",
                Window.into(GlobalWindows())
            )
                .apply(KFilter.by { _ -> false })

        //TODO
        // impulse + watermark only
        val labelsWithoutInput: PCollection<KV<BoundedWindow, String>> =
            PCollectionList.of(impulseString)
                .and(inputWatermarkOnly)
                .apply(
                    "Flatten [impulseString] with [inputWatermarkOnly]",
                    Flatten.pCollections()
                )
                .apply("Map To Empty String Key",
                    MapElements.into(strings() kvs strings())
                        .kVia { "" kv it }
                )
                .apply(
                    "Looping Timer For Window Labels",
                    LoopingTimerForWindowLabels(
                        loopDuration = windowFn.loopDuration,
                        startingTime = Instant.now() - 1.hours(),
                        windowingStrategy = windowingStrategy
                    ).parDo()
                )
                .setCoder(windowCoder)
                .apply("Debug", LogUtils.of())
                .apply("Map To Windowed Key With Empty Value",
                    MapElements.into(BoundedWindow::class.typeDescriptor() kvs strings())
                        .kVia { it kv "" }
                )
                .setCoder(windowCoder kvc StringUtf8Coder.of())

        val labelsWithInput =
            input.apply(Reify.windows())
                .apply("Map To Windowed Key With Value",
                    MapElements.into(BoundedWindow::class.typeDescriptor() kvs strings())
                        .kVia { it.window kv it.value }
                )
                .setCoder(windowCoder kvc StringUtf8Coder.of())
                .apply(
                    "LabelsWIthInput__GlobalWindowing",
                    Window.into(GlobalWindows())
                )

        val result = PCollectionList.of(labelsWithoutInput)
            .and(labelsWithInput)
            .apply(
                "Flatten [labelsWithoutInput] with [labelsWithInput]",
                Flatten.pCollections()
            )
            .apply(
                "Split Droppable Data",
                SplitDroppableDataFn(
                    allowedLateness = windowingStrategy.allowedLateness
                ).parDo().withOutputTags(mainOutput, TupleTagList.of(droppableOutput))
            )

        return PCollectionTuple.of(mainOutput, rewindow(result.get(mainOutput), windowingStrategy))
            .and(droppableOutput, result.get(droppableOutput))
    }

    @Suppress("UNUSED")
    private class LoopingTimerForWindowLabels(
        private val loopDuration: Duration,
        private val startingTime: Instant,
        private val windowingStrategy: WindowingStrategy<*, BoundedWindow>,
    ) : DoFn<KV<String, String>, BoundedWindow>() {

        private val triggeredCounter: Counter =
            Metrics.counter(LoopingTimerForWindowLabels::class.java, "triggered_counter")

        companion object {
            private val LOG = LoopingTimerForWindowLabels::class.logger()
            private const val LOOPING_TIMER = "looping_timer"
            private const val LAST_FIRE_TIMESTAMP = "last_fire_timestamp"
        }

        @TimerId(LOOPING_TIMER)
        private val loopingTimerSpec: TimerSpec =
            TimerSpecs.timer(TimeDomain.EVENT_TIME)

        @StateId(LAST_FIRE_TIMESTAMP)
        private val lastFireTimestampSpec: StateSpec<ValueState<Instant>> =
            StateSpecs.value()

        /**
         * This method only trigger one time because
         * - 1. one of input source is [Impulse]. and only one byte array triggered.
         * - 2. and other input source is only watermark, so there is no input elements. see [KFilter.by]
         */
        @ProcessElement
        fun process(
            @Element input: KV<String, String>,
            @TimerId(LOOPING_TIMER) loopingTimer: Timer,
            @StateId(LAST_FIRE_TIMESTAMP) lastFireTimestampState: ValueState<Instant>,
        ) {
            LOG.info("[@ProcessElement] element {} arrived at {}", input, Instant.now())
            lastFireTimestampState.write(startingTime).also { LOG.info("[@ProcessElement] last_fire_timestamp set") }
            loopingTimer.set(startingTime).also { LOG.info("[@ProcessElement] looping_timer set") }
            this.triggeredCounter.inc()
        }

        @OnTimer(LOOPING_TIMER)
        fun onLoopingTimer(
            @TimerId(LOOPING_TIMER) loopingTimer: Timer,
            @StateId(LAST_FIRE_TIMESTAMP) lastTimerTimestampState: ValueState<Instant>,
            context: OnTimerContext,
            output: OutputReceiver<BoundedWindow>,
        ) {
            LOG.info("[@OnTimer] looping_timer triggered at {}", Instant.now())
            val lastFired = lastTimerTimestampState.read()
            check(lastFired != null) { "state [last_fire_timestamp]'s value is null." }

            val currentTimestamp = context.fireTimestamp()
            lastTimerTimestampState.write(currentTimestamp)
            if (currentTimestamp.isBefore(GlobalWindow.INSTANCE.maxTimestamp())) {
                loopingTimer.withOutputTimestamp(currentTimestamp).offset(loopDuration).setRelative()
                    .also { LOG.info("[@OnTimer] looping_timer set at {}", Instant.now()) }

                this.outputWindowWithInterval(output, lastFired, currentTimestamp)
            }
        }

        private fun outputWindowWithInterval(
            output: OutputReceiver<BoundedWindow>,
            minStamp: Instant,
            maxStamp: Instant,
        ) {
            var timestamp = minStamp
            while (timestamp.isBefore(maxStamp)) {
                @Suppress("UNCHECKED_CAST")
                (windowingStrategy as WindowingStrategy<Any, BoundedWindow>).windowFn
                    .let { windowFn: WindowFn<Any, BoundedWindow> ->
                        windowFn.assignWindows(this.newContext(windowFn, timestamp))
                    }
                    .also { LOG.info("[@OnTimer] assign windows {}", it) }
                    .forEach(output::output)
                timestamp += this.loopDuration
            }
        }

        fun <T, W : BoundedWindow> WindowFn<T, W>.createAssignContext(
            timestamp: Instant,
        ): WindowFn<T, W>.AssignContext =
            object : WindowFn<T, W>.AssignContext() {
                override fun element(): T? = null

                override fun timestamp(): Instant = timestamp

                @Suppress("CAST_NEVER_SUCCEEDS")
                override fun window(): W = null as W
            }

        private fun <T : Any> newContext(
            windowFn: WindowFn<T, BoundedWindow>,
            timestamp: Instant,
        ): WindowFn<T, BoundedWindow>.AssignContext =
            windowFn.createAssignContext(timestamp)

    }

    @Suppress("UNUSED")
    private class SplitDroppableDataFn(
        private val allowedLateness: Duration,
    ) : DoFn<KV<BoundedWindow, String>, String>() {

        companion object {
            private val LOG = SplitDroppableDataFn::class.logger()

            private const val BUFFER = "buffer"
            private const val BUFFER_FLUSH_TIMER = "buffer_flush_timer"
            private const val WINDOW_CLOSED = "window_closed"
            private const val WINDOW_GC_TIMER = "window_gc_timer"

            private fun logTimerTriggered(timerName: String): Unit =
                LOG.info("[@OnTimer] timer [{}] triggered at {}", timerName, Instant.now())

            private fun logTimerInitialized(timerName: String): Unit =
                LOG.info("[SplitDroppableDataFn] timer [{}] initialized at {}", timerName, Instant.now())

            private fun logStateInitialized(stateName: String): Unit =
                LOG.info("[SplitDroppableDataFn] state [{}] initialized at {}", stateName, Instant.now())
        }

        @StateId(BUFFER)
        private val bufferSpec: StateSpec<BagState<KV<Instant, String>>> =
            StateSpecs.bag(KvCoder.of(InstantCoder.of(), StringUtf8Coder.of()))
                .also { logStateInitialized(BUFFER) }

        @TimerId(BUFFER_FLUSH_TIMER)
        private val bufferFlushTimerSpec: TimerSpec =
            TimerSpecs.timer(TimeDomain.EVENT_TIME)
                .also { logTimerInitialized(BUFFER_FLUSH_TIMER) }

        @StateId(WINDOW_CLOSED)
        private val windowClosedSpec: StateSpec<ValueState<Boolean>> =
            StateSpecs.value<Boolean>()
                .also { logStateInitialized(WINDOW_CLOSED) }

        @TimerId(WINDOW_GC_TIMER)
        private val windowGcTimer: TimerSpec =
            TimerSpecs.timer(TimeDomain.EVENT_TIME)
                .also { logTimerInitialized(WINDOW_GC_TIMER) }

        @Deprecated(
            "Deprecated in Java",
            ReplaceWith("1.hours()", "com.icloud.extensions.hours")
        )
        override fun getAllowedTimestampSkew(): Duration = 1.hours()

        @ProcessElement
        fun process(
            @Element element: KV<BoundedWindow, String>,
            @StateId(WINDOW_CLOSED) windowClosedState: ValueState<Boolean>,
            @StateId(BUFFER) bufferState: BagState<KV<Instant, String>>,
            @TimerId(WINDOW_GC_TIMER) windowGcTimer: Timer,
            @TimerId(BUFFER_FLUSH_TIMER) bufferFlushTimer: Timer,
            @Timestamp timestamp: Instant,
            output: MultiOutputReceiver,
        ) {
            val (key: BoundedWindow, value: String) = element
            bufferState.readLater()
            val isWindowClosed = windowClosedState.read()
            if (isWindowClosed == null) {
                val lastNotDroppable = key.maxTimestamp() + this.allowedLateness
                val gcTime = lastNotDroppable + 1

                windowGcTimer.withOutputTimestamp(lastNotDroppable).set(gcTime)
                bufferFlushTimer.offset(Duration.ZERO).setRelative()
                    .also { LOG.info("[@ProcessElement] timer {} set...", BUFFER_FLUSH_TIMER) }
                bufferState.add(timestamp kv value)
                    .also { LOG.info("[@ProcessElement] value {} buffer added...", value) }
            } else {
                this.flushOutput(
                    element = value,
                    timestamp = timestamp,
                    closed = isWindowClosed,
                    output = output
                )
            }
        }

        @OnTimer(BUFFER_FLUSH_TIMER)
        fun onBufferFlushTimer(
            context: OnTimerContext,
            @Key window: BoundedWindow,
            @StateId(BUFFER) bufferState: BagState<KV<Instant, String>>,
            @StateId(WINDOW_CLOSED) windowClosedState: ValueState<Boolean>,
            output: MultiOutputReceiver,
        ) {
            logTimerTriggered(BUFFER_FLUSH_TIMER)
            val closed =
                windowClosedState.read() ?: (window.maxTimestamp() + allowedLateness).isBefore(context.fireTimestamp())

            windowClosedState.write(closed)
                .also {
                    LOG.info("[@OnTimer] {} state [window_closed] updated at {}", BUFFER_FLUSH_TIMER, Instant.now())
                }

            this.flushBuffer(
                bufferState = bufferState,
                closed = closed,
                output = output
            )
        }

        @OnTimer(WINDOW_GC_TIMER)
        fun onWindowGcTimer(
            @StateId(WINDOW_CLOSED) windowClosedState: ValueState<Boolean>,
            @StateId(BUFFER) bufferState: BagState<KV<Instant, String>>,
            output: MultiOutputReceiver,
        ) {
            logTimerTriggered(WINDOW_GC_TIMER)
            windowClosedState.write(true)
            this.flushBuffer(
                bufferState = bufferState,
                closed = true,
                output = output
            )
        }


        private fun flushOutput(
            element: String,
            timestamp: Instant,
            closed: Boolean,
            output: MultiOutputReceiver,
        ) {
            if (element.isNotBlank()) {
                if (closed) {
                    output.get(droppableOutput).outputWithTimestamp(element, timestamp)
                } else {
                    output.get(mainOutput).outputWithTimestamp(element, timestamp)
                }
            }
        }

        private fun flushBuffer(
            bufferState: BagState<KV<Instant, String>>,
            closed: Boolean,
            output: MultiOutputReceiver,
        ) {
            bufferState.read()
                .forEach { (timestamp: Instant, value: String) ->
                    this.flushOutput(value, timestamp, closed, output)
                }

            bufferState.clear()
                .also { LOG.info("[flushBuffer] state buffer cleared at {}", Instant.now()) }
        }
    }
}
