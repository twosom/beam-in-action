package com.icloud.droppable

import com.icloud.droppable.DroppableDataFilter
import com.icloud.extensions.seconds
import com.icloud.extensions.timestampedValue
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.transforms.windowing.IntervalWindow
import org.apache.beam.sdk.transforms.windowing.Window
import org.joda.time.Instant
import org.junit.Test


class DroppableDataFilterTest : AbstractPipelineTest() {


    @Test
    fun `Test Droppable Data`() {
        val rawNow = Instant.now()

        val now = rawNow.minus(rawNow.millis % 60_000)

        val stream = TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(now) // now
            .addElements("a".timestampedValue(now + 1)) // fine, before watermark - on time
            .advanceWatermarkTo(now + 1999) // now + 1.9 sec
            .addElements("b".timestampedValue(now + 999)) // late, but within allowed lateness
            .advanceWatermarkTo(now + 2000) // now + 2.0 sec
            .addElements("c".timestampedValue(now)) // droppable
            .advanceWatermarkToInfinity()

        val input = p.apply(stream)
            .apply(
                "Test_Windowing",
                Window.into<String>(FixedWindows.of(1.seconds()))
                    .withAllowedLateness(1.seconds())
                    .discardingFiredPanes()
            )

        val split = DroppableDataFilter.splitDroppable(input)
        PAssert.that(split.get(DroppableDataFilter.mainOutput))
            .inWindow(IntervalWindow(now, now + 1000))
            .containsInAnyOrder("a", "b")

        PAssert.that(split.get(DroppableDataFilter.droppableOutput))
            .inWindow(GlobalWindow.INSTANCE)
            .containsInAnyOrder("c")

        p.run().waitUntilFinish()

    }
}