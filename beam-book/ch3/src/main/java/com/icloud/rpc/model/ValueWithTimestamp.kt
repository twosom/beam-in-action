package com.icloud.rpc.model

import org.joda.time.Instant

data class ValueWithTimestamp<T>(
    val value: T,
    val timestamp: Instant,
) {
    companion object {
        fun <T> of(
            value: T,
            timestamp: Instant,
        ): ValueWithTimestamp<T> = ValueWithTimestamp(value, timestamp)
    }
}