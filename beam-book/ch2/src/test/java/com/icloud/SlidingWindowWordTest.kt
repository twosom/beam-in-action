package com.icloud

import com.icloud.extensions.tv
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestStream
import org.joda.time.Instant
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SlidingWindowWordTest {
    @JvmField
    @Rule
    val p: TestPipeline = TestPipeline.create()

    private val baseTime: Instant = Instant(0)


    @Test
    fun `Test Average Word Calculation`() {
        val createStream = TestStream.create(StringUtf8Coder.of())

            //sliding a start
            .addElements("a" tv baseTime)
            .addElements("bb" tv baseTime.plus(1999))
            // sliding a end

            // sliding b start
            .addElements("ccc" tv baseTime.plus(5000))
            // sliding b end

            // sliding c start
            .addElements("dddd" tv baseTime.plus(10000))
            // sliding c end
            .advanceWatermarkToInfinity()

        val input = p.apply(createStream)

        val averages = SlidingWindowWord.calculateWordLength(input)
        PAssert.that(averages)
            .containsInAnyOrder(
                1.5, 1.5,
                2.0, 2.0, 2.0,
                3.5, 3.5,
                4.0, 4.0, 4.0
            )

        p.run()
    }

}