package com.icloud

import com.icloud.coder.MetricCoder
import com.icloud.coder.PositionCoder
import com.icloud.model.Metric
import com.icloud.model.Position
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.state.*
import org.apache.beam.sdk.state.Timer
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Duration
import org.joda.time.Instant
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.*
import kotlin.math.min

internal typealias KeyedPosition = KV<String, Position>
internal typealias KeyedMetric = KV<String, Metric>

class ToMetric
    : PTransform<PCollection<KeyedPosition>, PCollection<KeyedMetric>>() {

    override fun expand(
        input: PCollection<KeyedPosition>,
    ): PCollection<KeyedMetric> =
        input.apply("globalWindow", Window.into(GlobalWindows()))
            .apply("generateInMinutesIntervals", ParDo.of(ReportInMinuteIntervalsFn()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), MetricCoder.of()))

    private class ReportInMinuteIntervalsFn
        : DoFn<KeyedPosition, KeyedMetric>() {

        companion object {
            private val LOG: Logger = LoggerFactory.getLogger(ReportInMinuteIntervalsFn::class.java)
        }


        @TimerId("flush_position")
        private val flushPositionTimerSpec: TimerSpec =
            TimerSpecs.timer(TimeDomain.EVENT_TIME)

        //TODO
        // we can change this to CombineState
        @StateId("min_timer_outputs_ts")
        private val minTImerOutputTs: StateSpec<ValueState<Instant>> = StateSpecs.value()

        @StateId("positions")
        private val cachedPositionsSpec: StateSpec<BagState<Position>> =
            StateSpecs.bag(PositionCoder.of())

        @ProcessElement
        fun process(
            @Element element: KeyedPosition,
            @Timestamp ts: Instant,
            @StateId("positions") positionsState: BagState<Position>,
            @StateId("min_timer_outputs_ts") minTimerState: ValueState<Instant>,
            @TimerId("flush_position") flushTimer: Timer,
        ) {
            val minTimerTs = minTimerState.read() ?: BoundedWindow.TIMESTAMP_MAX_VALUE
            if (ts.isBefore(minTimerTs)) {
                flushTimer.withOutputTimestamp(ts).set(ts)
                minTimerState.write(ts)
            }

            positionsState.add(element.value)
        }

        @OnTimer("flush_position")
        fun onFlushPositionTimer(
            @Key key: String,
            @StateId("positions") positionsState: BagState<Position>,
            @StateId("min_timer_outputs_ts") minTimerState: ValueState<Instant>,
            @TimerId("flush_position") flushTimer: Timer,
            output: OutputReceiver<KeyedMetric>,
        ) {
            minTimerState.write(BoundedWindow.TIMESTAMP_MIN_VALUE)
                .also { LOG.info("[@OnTimer] state [min_timer_state] set as [BoundedWindow.TIMESTAMP_MIN_VALUE]") }

            val queue: PriorityQueue<Position> =
                PriorityQueue(Comparator.comparing(Position::timestamp))

            val ts = flushTimer.currentRelativeTime

            val keep = arrayListOf<Position>()
            var minTsToKeep = BoundedWindow.TIMESTAMP_MAX_VALUE.millis

            for (pos in positionsState.read()) {
                if (pos.timestamp < ts.millis) {
                    queue.add(pos)
                } else {
                    if (minTsToKeep > pos.timestamp) {
                        minTsToKeep = pos.timestamp
                            .also { LOG.info("[@OnTimer] minTsToKeep changed") }
                    }
                    keep.add(pos)
                }
            }

            if (!queue.isEmpty()) {
                val first = queue.poll()
                val last =
                    computeMinuteMetrics(first = first, queue = queue) {
                        output.outputWithTimestamp(KV.of(key, it.value), it.timestamp)
                    }
                keep.add(last)
                minTsToKeep = last.timestamp
            }
            positionsState.clear()
            keep.forEach(positionsState::add)
            setNewLoopTimer(timer = flushTimer, currentTimestamp = ts, minTsToKeep = minTsToKeep)
        }

        private fun computeMinuteMetrics(
            first: Position,
            queue: PriorityQueue<Position>,
            outputFn: (TimestampedValue<Metric>) -> Unit,
        ): Position {
            var current = first
            while (!queue.isEmpty()) {
                val pos = queue.poll()
                val timeDiffMs = pos.timestamp - current.timestamp

                if (timeDiffMs > 0) {
                    val lattDiff = pos.latitude - current.latitude
                    val lonDiff = pos.longitude - current.longitude
                    val distance = pos distance current
                    val avgSpeedMetricPerSec = (distance * 1000) / timeDiffMs

                    while (current.timestamp < pos.timestamp) {
                        val deltaMs = min(
                            pos.timestamp - current.timestamp,
                            60_000 - current.timestamp % 60_000
                        )
                        val nextPos = current.move(lattDiff, lonDiff, avgSpeedMetricPerSec, deltaMs)
                        val metric = Metric(nextPos distance current, deltaMs)
                        outputFn(TimestampedValue.of(metric, Instant.ofEpochMilli(nextPos.timestamp)))
                        current = nextPos
                    }
                }
            }
            return current
        }

        private fun setNewLoopTimer(
            timer: Timer,
            currentTimestamp: Instant,
            minTsToKeep: Long,
        ) {
            var mtk = minTsToKeep
            if (currentTimestamp.millis > minTsToKeep + 300_000) {
                mtk = currentTimestamp.millis
            }
            if (timer.currentRelativeTime.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
                timer
                    .withOutputTimestamp(Instant.ofEpochMilli(mtk))
                    .offset(Duration.ZERO)
                    .align(Duration.standardMinutes(1L))
                    .setRelative()
                    .also { LOG.info("[setNewLoopTimer] timer set at {}", Instant.now()) }
            }
        }
    }


}