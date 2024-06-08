package com.icloud.service

import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

class PlaceholderExternalService {
    companion object {
        fun readTestData(timestamp: Instant): Map<String, String> =
            mapOf(
                "Key_A" to timestamp.toString(DateTimeFormat.forPattern("HH:mm:ss")),
                "Key_B" to timestamp.toString(DateTimeFormat.forPattern("HH:mm:ss"))
            )
    }
}