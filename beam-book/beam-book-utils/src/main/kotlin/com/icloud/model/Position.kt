package com.icloud.model

import com.google.common.base.Preconditions
import kotlin.math.cos
import kotlin.math.sqrt


data class Position(
    val latitude: Double,
    val longitude: Double,
    val timestamp: Long,
) {
    companion object {
        const val EARTH_DIAMETER: Long = 6_371_000
        const val METER_TO_ANGLE: Double = 180.0 / (EARTH_DIAMETER * Math.PI)

        @JvmStatic
        fun parseFrom(tsv: String): Position? {
            var parts = tsv.split("\t")
            if (parts.size < 3) return null
            if (parts.size > 3) {
                parts = parts.subList(parts.size - 3, parts.size)
            }
            return invoke(parts)
        }

        @JvmStatic
        fun random(timestamp: Long) = Position(
            latitude = (Math.random() - 0.5) * 180,
            longitude = (Math.random() - 0.5) * 180,
            timestamp = timestamp
        )


        operator fun invoke(parts: List<String>): Position {
            return Position(
                parts[0].toDouble(),
                parts[1].toDouble(),
                parts[2].toLong()
            )
        }

        private fun calculateDistanceOfPositions(
            first: Position,
            second: Position,
        ): Double {
            val deltaLatitude = first.latitude deltaRadians second.latitude
            val deltaLongitude = first.longitude deltaRadians second.longitude
            val latitudeIncInMeters = deltaLatitude.calculateDelta()
            val longitudeIncInMeters = deltaLongitude.calculateDelta()
            return EARTH_DIAMETER * sqrt(
                latitudeIncInMeters * latitudeIncInMeters
                        + longitudeIncInMeters * longitudeIncInMeters
            )
        }

        private fun Double.calculateDelta() = sqrt(2 * (1 - cos(this)))

        private infix fun Double.deltaRadians(target: Double) =
            (this - target) * Math.PI / 180
    }

    infix fun distance(other: Position): Double = calculateDistanceOfPositions(this, other)

    fun move(
        latitudeDirection: Double,
        longitudeDirection: Double,
        speedMeterPerSec: Double,
        timeMillis: Long,
    ): Position {
        val desiredStepSize = (speedMeterPerSec * timeMillis) / 1000 * METER_TO_ANGLE;
        val deltaSize = sqrt(latitudeDirection * latitudeDirection + longitudeDirection * longitudeDirection)
        Preconditions.checkArgument(deltaSize > 0)
        return Position(
            this.latitude + latitudeDirection / deltaSize * desiredStepSize,
            this.longitude + longitudeDirection / deltaSize * desiredStepSize,
            this.timestamp + timeMillis
        )
    }

}
