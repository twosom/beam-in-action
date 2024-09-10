package com.icloud.droppable

import com.icloud.extensions.seconds
import com.icloud.extensions.timestampedValue
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Flatten
import org.apache.beam.sdk.transforms.Sum
import org.apache.beam.sdk.transforms.windowing.*
import org.apache.beam.sdk.values.PCollectionList
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Duration
import org.joda.time.Instant
import org.junit.Assert.assertTrue
import org.junit.Test
import java.security.SecureRandom
import java.util.*


class DroppableDataFilter2Test : AbstractPipelineTest() {

    private val random: Random by lazy { SecureRandom() }

    @Test
    fun `Test Droppable Data`() {
        val rawNow = Instant.now()

        val now = rawNow - (rawNow.millis % 60_000)

        val stream = TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(now)
            .addElements("a".timestampedValue(now + 1)) // before watermark
            .advanceWatermarkTo(now + 1_000) // increase event time 1 second
            .advanceWatermarkTo(now + 1_999) // increase event time 0.999 second
            .addElements("b".timestampedValue(now + 999)) // late data because data is before watermark 1 second
            .addElements("c".timestampedValue(now + 500)) // late data because data is before watermark 1.499 second
            .advanceWatermarkTo(now + 5_000) // increase event time 3.001 second
            .addElements("d".timestampedValue(now)) // droppable
            .advanceWatermarkToInfinity()

        val input = p.apply(stream)
            .apply(
                "TestStream__FixedWindowing__1Seconds",
                Window.into<String>(FixedWindows.of(1.seconds()))
                    .withAllowedLateness(1.seconds())
                    .discardingFiredPanes()
            )

        val split = DroppableDataFilter2.splitDroppable(input)

        PAssert.that(split.get(DroppableDataFilter2.mainOutput))
            .inWindow(IntervalWindow(now, now + 1_000))
            .containsInAnyOrder("a", "b", "c")

        PAssert.that(split.get(DroppableDataFilter2.droppableOutput))
            .inWindow(GlobalWindow.INSTANCE)
            .containsInAnyOrder("d")

        p.run().waitUntilFinish()
    }

    @Test
    fun `Test Contract`() {
        val numElements = 20L
        val now: Instant = Instant.now()

        var builder = TestStream.create(StringUtf8Coder.of())
        for (i in 0 until numElements) {
            val currentNow = now + (100 * i)
            val value = currentNow.newValue(i)

            builder = builder.addElements(value).advanceWatermarkTo(currentNow)
        }

        val input = p.apply(builder.advanceWatermarkToInfinity())
            .apply(
                "InputStream__FixedWindowing__1Seconds",
                Window.into(FixedWindows.of(1.seconds()))
            )

        val result = DroppableDataFilter2.splitDroppable(input)

        val mainCounted = result.get(DroppableDataFilter2.mainOutput)
            .apply(
                "MainOutput__Combine",
                Combine.globally(Count.combineFn<String>()).withoutDefaults()
            )
            .apply(
                "MainOutput__GlobalWindowing",
                Window.into<Long>(GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.ZERO)
            )
            .apply(
                "MainOutput__Sum",
                Sum.longsGlobally()
            )

        val droppableCounted = result.get(DroppableDataFilter2.droppableOutput)
            .apply(
                "DroppableOutput__GlobalWindowing",
                Window.into<String>(GlobalWindows())
                    .triggering(AfterWatermark.pastEndOfWindow())
                    .accumulatingFiredPanes()
                    .withAllowedLateness(Duration.ZERO)
            )
            .apply("DroppableOutput__Count", Count.globally())


        val allElements = PCollectionList.of(mainCounted)
            .and(droppableCounted)
            .apply(Flatten.pCollections())
            .apply(Sum.longsGlobally())

        PAssert.that(allElements).containsInAnyOrder(numElements)

        PAssert.that(droppableCounted)
            .satisfies {
                assertTrue(it.count() > 0)
                return@satisfies null
            }

        p.run().waitUntilFinish()
    }


    private fun Instant.newValue(pos: Long): TimestampedValue<String> {
        if (random.nextBoolean()) {
            val lateDuration = (random.nextInt(1800) + 60).toLong().seconds()
            return pos.toString().timestampedValue(this.minus(lateDuration))
        }
        return pos.toString().timestampedValue(this)
    }
}