package com.icloud

import com.icloud.coder.PositionCoder
import com.icloud.extensions.kVia
import com.icloud.model.Position
import com.icloud.model.Position.Companion.EARTH_DIAMETER
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.testing.TestPipeline
import org.apache.beam.sdk.testing.TestStream
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.TimestampedValue
import org.apache.beam.sdk.values.TypeDescriptors.strings
import org.joda.time.Instant
import org.junit.Rule
import org.junit.Test
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.io.BufferedReader
import java.io.InputStreamReader
import kotlin.math.round
import kotlin.math.sqrt

@RunWith(JUnit4::class)
class SportTrackerTest {

    @Rule
    @JvmField
    val pipeline: TestPipeline = TestPipeline.create()

    @Test
    fun `test GPS delta length calculation`() {
        val now = System.currentTimeMillis()
        val first = Position(64.53165930148285, -17.3172239914558, now)
        val second = Position(64.53168645625584, -17.316554613536688, now)

        val distance = first distance second
        assertTrue(distance < 80, "Expected less than 80 meters, got $distance")
        assertTrue(distance > 30, "Expected more than 30 meters, got $distance")

        assertEquals(distance, second distance first, 0.001)
        assertEquals(0.0, first distance first, 0.001)
        assertEquals(0.0, second distance second, 0.001)

        assertEquals(
            2 * EARTH_DIAMETER.toDouble(),
            Position(90.0, 0.0, now) distance Position(-90.0, 0.0, now),
            0.01
        )

        assertEquals(
            2 * EARTH_DIAMETER.toDouble(),
            Position(0.0, 90.0, now) distance Position(0.0, -90.0, now),
            0.01
        )

        assertEquals(
            sqrt(2.0) * EARTH_DIAMETER,
            Position(90.0, 0.0, now) distance Position(0.0, 0.0, now),
            0.01
        )

        val circumference = 2.0 * Math.PI * EARTH_DIAMETER
        val angle100 = 100.0 / circumference * 360
        val zero = Position(.0, .0, now)
        assertEquals(100.0, zero distance Position(.0, angle100, now), 0.0001)
    }

    @Test
    fun `test GPS delta length calculation 2`() {
        val now = System.currentTimeMillis()
        val first = Position(.0, .0, now)
        var second = Position(.0, .1, now)
        assertEquals(11119.0, first distance second, 1.0)
        assertEquals(11119.0, second distance first, 1.0)
        second = Position(.001, .001, now)
        assertEquals(sqrt(2.0) * 111, first distance second, 1.0)
    }

    @Test
    fun `test pipeline`() {
        SportTracker.registerCoders(pipeline)
        val input = loadData()

        val result = SportTracker.computeTrackMetrics(pipeline.apply(asTestStream(input)))
            .apply(
                MapElements.into(strings())
                    .kVia { kv ->
                        "%s:%d,%d".format(
                            kv.key,
                            round(kv.value.length).toInt(),
                            kv.value.duration.toInt()
                        )
                    }
            )

        PAssert.that(result)
            .inOnTimePane(GlobalWindow.INSTANCE)
            .containsInAnyOrder("track1:614,257", "track2:5641,1262")

        pipeline.run()
    }

    private fun asTestStream(input: List<KV<String, Position>>): TestStream<KV<String, Position>> {
        var builder =
            TestStream.create(KvCoder.of(StringUtf8Coder.of(), PositionCoder()))

        for (kv in input) {
            builder = builder.addElements(
                TimestampedValue.of(kv, Instant.ofEpochMilli(kv.value.timestamp))
            )
        }

        return builder.advanceWatermarkToInfinity()
    }

    private fun loadData(): List<KV<String, Position>> =
        BufferedReader(InputStreamReader(this.javaClass.classLoader.getResourceAsStream("test-tracker-data.txt")!!))
            .use { reader ->
                reader.lineSequence()
                    .map { it.split("\t") }
                    .filter { it.size == 4 }
                    .map {
                        KV.of(
                            it[0],
                            Position(
                                it[1].toDouble(),
                                it[2].toDouble(),
                                it[3].toLong()
                            )
                        )
                    }
                    .toList()
            }

}