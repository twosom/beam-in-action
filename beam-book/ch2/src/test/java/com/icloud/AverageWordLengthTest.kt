package com.icloud

import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.values.PCollection
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class AverageWordLengthTest {

    @JvmField
    @Rule
    val p: TestPipeline = TestPipeline.create()

    @Test
    fun `Test Average Word Calculation with 'disableDefault'`() {
        val average = AverageWordLength.calculateWordLength(
            createInput(),
            true
        )
            .apply(LogUtils.of(true))

        PAssert.that(average)
            .containsInAnyOrder(1.0, 1.5, 2.0)

        p.run()
    }

    private fun createInput(): PCollection<String> {
        val createStream = TestStream.create(StringUtf8Coder.of())
            .addElements("a")
            .addElements("bb")
            .addElements("ccc")
            .advanceWatermarkToInfinity()

        return p.apply(createStream)
    }

}