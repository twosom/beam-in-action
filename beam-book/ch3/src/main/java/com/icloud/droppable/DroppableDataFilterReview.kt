package com.icloud.droppable

import com.icloud.PipelineUtils
import com.icloud.extensions.*
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.state.*
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.*
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.joda.time.Duration
import org.joda.time.Instant

/**
 * Combining of [Impulse] with [MapElements]
 */
private class ImpulseString
private constructor() : PTransform<PBegin, PCollection<String>>() {

    companion object {
        @JvmStatic
        fun of(): ImpulseString = ImpulseString()
    }

    override fun expand(input: PBegin): PCollection<String> =
        input.apply(Impulse.create())
            .apply(MapElements.into(strings()).kVia { "" })
}


private class LoopingTimerForWindowLabels(
    private val loopDuration: Duration,
    private val startingTime: Instant,
    private val windowingStrategy: WindowingStrategy<*, BoundedWindow>,
) : DoFn<KV<String, String>, BoundedWindow>() {

    companion object {
        private val LOG = LoopingTimerForWindowLabels::class.logger()
    }

    @StateId("last_timer_timestamp")
    private val lastTimerTimestampSpec: StateSpec<ValueState<Instant>> =
        StateSpecs.value()

    @TimerId("looping_timer")
    private val loopingTimerSpec: TimerSpec =
        TimerSpecs.timer(TimeDomain.EVENT_TIME)


    @ProcessElement
    fun process(
        @Element element: KV<String, String>,
        @TimerId("looping_timer") loopingTimer: Timer,
        @StateId("last_timer_timestamp") lastTimerTimestampState: ValueState<Instant>,
    ) {
        LOG.info("[@ProcessElement] element {} arrived at {}", element, Instant.now())

        lastTimerTimestampState.write(this.startingTime)
            .also { LOG.info("[@ProcessElement] set state [last_timer_timestamp] at {}", Instant.now()) }

        loopingTimer.set(this.startingTime)
            .also { LOG.info("[@ProcessElement] set timer [looping_timer] at {}", Instant.now()) }
    }


    /**
     * [loopingTimer] 트리거 시 실행되는 메소드
     *
     * 절차는 다음과 같음
     *
     * 1. [lastTimerTimestampState] 에서 직전에 실행한 타이머의 타임스탬프 가져옴
     * 1-a. [lastTimerTimestampState] 에서 가져온 값은 null이어서는 안됨.
     *
     * 2. [DoFn.OnTimerContext.fireTimestamp] 에서 현재 타이머의 트리거 된 타임스탬프를 가져옴.
     * 3. [lastTimerTimestampState] 에 현재 타이머의 트리거 된 타임스탬프 저장
     * 4. [GlobalWindow.maxTimestamp] 보다 이전이면 [loopingTimer] 다시 세팅
     *          (타이머에는 현재 타임스탬프를 출력 타임스탬프로 설정하고, [loopDuration] 뒤에 타이머가 트리거 되도록 설정)
     * 5. 바로 직전 타임스탬프 ~ 현재 타임스탬프를 [loopDuration] 만큼씩 순회하며 [WindowFn.assignWindows] 메소드 실행
     */
    @OnTimer("looping_timer")
    fun onLoopingTimer(
        @TimerId("looping_timer") loopingTimer: Timer,
        @StateId("last_timer_timestamp") lastTimerTimestampState: ValueState<Instant>,
        context: OnTimerContext,
        outputReceiver: OutputReceiver<BoundedWindow>,
    ) {
        LOG.info("[@OnTimer] timer [looping_timer] triggered at {}", Instant.now())
        val lastTimestamp = lastTimerTimestampState.read()
        check(lastTimestamp != null) { "state [last_timer_timestamp] cannot be null'" }

        val currentTimestamp = context.fireTimestamp()
            .also { lastTimerTimestampState.write(it) }
            .also { LOG.info("[@OnTimer] set state [last_timer_timestamp] at {}", Instant.now()) }

        if (currentTimestamp.isBefore(GlobalWindow.INSTANCE.maxTimestamp())) {
            loopingTimer.withOutputTimestamp(currentTimestamp).offset(this.loopDuration).setRelative()
                .also { LOG.info("[@OnTimer] set timer [looping_timer] at {}", Instant.now()) }

            this.outputWindowWithInterval(outputReceiver, lastTimestamp, currentTimestamp)
        }
    }

    private fun outputWindowWithInterval(
        output: OutputReceiver<BoundedWindow>,
        minTimestamp: Instant,
        maxTimestamp: Instant,
    ) {
        var timestamp = minTimestamp

        while (timestamp.isBefore(maxTimestamp)) {
            @Suppress("UNCHECKED_CAST")
            val windowFn =
                (this.windowingStrategy as WindowingStrategy<Any, BoundedWindow>).windowFn

            windowFn.assignWindows(windowFn.createAssignContext(timestamp))
                .forEach { output.output(it) }

            timestamp += this.loopDuration
                .also { LOG.info("[outputWindowWithInterval] loopDuration added at {}", Instant.now()) }
        }
    }

    private fun <T, W : BoundedWindow> WindowFn<T, W>.createAssignContext(
        timestamp: Instant,
    ): WindowFn<T, W>.AssignContext =
        object : WindowFn<T, W>.AssignContext() {
            override fun element(): T? = null

            override fun timestamp() = timestamp

            @Suppress("CAST_NEVER_SUCCEEDS")
            override fun window(): W = null as W

        }

}


object DroppableDataFilterReview : AbstractDroppableDataFilter() {


    private val WINDOW_SIZE = 10.minutes()
    private val ALLOWED_LATENESS = 30.seconds()

    @JvmStatic
    fun main(args: Array<String>) {
        val (p, options) = PipelineUtils.from(args, Options::class.java)

        val input = p.readInput(options).apply(
            "Windowing into 10 Minutes FixedWindows",
            Window.into<String>(FixedWindows.of(WINDOW_SIZE))
                .withAllowedLateness(ALLOWED_LATENESS)
                .discardingFiredPanes()
        )


        val impulseString = input.pipeline.apply(ImpulseString.of())

        val onlyWatermark = input
            .apply(
                "InputGlobalWindowing", Window.into(GlobalWindows())
            )
            .apply("Filter Only Watermark", KFilter.by { false })

        val windowingStrategy: WindowingStrategy<String, BoundedWindow> =
            input.typedWindowingStrategy

        val windowCoder = windowingStrategy.windowFn.windowCoder()


        //TODO labels without input
        val labelsWithoutInput = PCollectionList
            .of(impulseString)
            .and(onlyWatermark)
            .apply(
                "Flatten", Flatten.pCollections()
            )
            .apply("Map To Empty KV",
                MapElements.into(strings() kvs strings())
                    .kVia { "" kv it }
            )
            .apply(
                "Looping Timer For Window Labels",
                LoopingTimerForWindowLabels(
                    loopDuration = windowingStrategy.loopDuration,
                    startingTime = Instant.now() - 1.hours(),
                    windowingStrategy = windowingStrategy
                ).parDo()
            )
            .setCoder(windowCoder)
            .apply(MapElements.into(BoundedWindow::class.typeDescriptor() kvs strings())
                .kVia { it kv "" })
            .setCoder(windowCoder kvc StringUtf8Coder.of())
            .apply(Window.into(GlobalWindows()))


        //TODO
        // labels with input
        val labelsWithInput = input.apply(Reify.windows())
            .apply(MapElements.into(BoundedWindow::class.typeDescriptor() kvs strings())
                .kVia { it.window kv it.value })
            .setCoder(windowCoder kvc StringUtf8Coder.of())
            .apply(Window.into(GlobalWindows()))

        PCollectionList.of(labelsWithoutInput)
            .and(labelsWithInput)
            .apply(Flatten.pCollections())



        p.run()
    }


    private val <T> WindowingStrategy<T, BoundedWindow>.loopDuration: Duration
        get() = when (val fn = this.windowFn) {
            is FixedWindows -> if (fn.size.millis > 5_000) 5.seconds() else fn.size
            else -> 1.seconds()
        }
}


