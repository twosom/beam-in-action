package com.icloud.rpc.model

import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.joda.time.Instant


data class ValueWithTimestampAndWindow<T>(
    val value: T,
    val timestamp: Instant,
    val window: BoundedWindow,
)