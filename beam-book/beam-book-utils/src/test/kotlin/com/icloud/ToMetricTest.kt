package com.icloud

import com.icloud.coder.PositionCoder
import com.icloud.model.Position
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.Reify
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TimestampedValue
import org.joda.time.Instant
import org.junit.Rule
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ToMetricTest {
    @Rule
    @JvmField
    val p: TestPipeline = TestPipeline.create()


    @Test
    fun `Test Computation Batch`() {
        val now = Instant.ofEpochMilli(1234567890000L)
        val initial = Position.random(now.millis)

        val input: PCollection<KeyedPosition> =
            p.apply(
                Create.timestamped(
                    timestamped(KV.of("foo", initial), now),
                    timestamped(
                        KV.of(
                            "foo",
                            initial.move(1.0, 1.0, 3.0, 120_000)
                        ), now + 120_000
                    ),
                ).withCoder(KvCoder.of(StringUtf8Coder.of(), PositionCoder.of()))
            )

        val result: PCollection<TimestampedValue<KeyedMetric>> = input.apply(ToMetric())
            .apply(Reify.timestamps())

        PAssert.that(result)
            .satisfies {
                for (timestampedValue: TimestampedValue<KeyedMetric> in it) {
                    when (timestampedValue.timestamp) {
                        now + 30_000 -> {
                            assertEquals("foo", timestampedValue.value.key)
                            assertEquals(90.0, timestampedValue.value.value.length, 0.0001)
                            assertEquals(30_000, timestampedValue.value.value.duration)
                        }

                        now + 90_000 -> {
                            assertEquals("foo", timestampedValue.value.key)
                            assertEquals(180.0, timestampedValue.value.value.length, 0.0001)
                            assertEquals(60_000, timestampedValue.value.value.duration)
                        }

                        now + 120_000 -> {
                            assertEquals("foo", timestampedValue.value.key)
                            assertEquals(90.0, timestampedValue.value.value.length, 0.0001)
                            assertEquals(30_000, timestampedValue.value.value.duration)
                        }

                        else -> {
                            fail("Unrecognized stamp ${timestampedValue.timestamp}, base $now")
                        }
                    }
                }
                null
            }

        p.run().waitUntilFinish()
    }

    @Test
    fun `Test Computation Stream`() {
        val now = Instant.ofEpochMilli(1234567890000L)
        val initial = Position.random(now.millis)

        val input: PCollection<KV<String, Position>> = p.apply(
            TestStream.create(KvCoder.of(StringUtf8Coder.of(), PositionCoder.of()))
                .addElements(timestamped(KV.of("foo", initial), now))
                .advanceWatermarkTo(now)
                .addElements(
                    timestamped(
                        KV.of(
                            "foo",
                            initial.move(1.0, 1.0, 3.0, 120_000)
                        ), now + 120_000
                    )
                )
                .advanceWatermarkTo(now + 300_000)
                .advanceWatermarkToInfinity()
        )

        val result = input.apply(ToMetric())
            .apply(Reify.timestamps())

        PAssert.that(result)
            .satisfies {
                for (timestampedValue: TimestampedValue<KeyedMetric> in it) {
                    when (timestampedValue.timestamp) {
                        now + 30_000 -> {
                            assertEquals("foo", timestampedValue.value.key)
                            assertEquals(90.0, timestampedValue.value.value.length, 0.0001)
                            assertEquals(30_000, timestampedValue.value.value.duration)
                        }

                        now + 90_000 -> {
                            assertEquals("foo", timestampedValue.value.key)
                            assertEquals(180.0, timestampedValue.value.value.length, 0.0001)
                            assertEquals(60_000, timestampedValue.value.value.duration)
                        }

                        now + 120_000 -> {
                            assertEquals("foo", timestampedValue.value.key)
                            assertEquals(90.0, timestampedValue.value.value.length, 0.0001)
                            assertEquals(30_000, timestampedValue.value.value.duration)
                        }

                        else -> {
                            fail("Unrecognized stamp ${timestampedValue.timestamp}, base $now")
                        }
                    }
                }
                null
            }

        p.run().waitUntilFinish()
    }

    private fun <InputT> timestamped(
        value: InputT,
        timestamp: Instant,
    ): TimestampedValue<InputT> = TimestampedValue.of(value, timestamp)
}