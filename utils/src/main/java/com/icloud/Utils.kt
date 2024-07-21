package com.icloud

import java.io.BufferedReader
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.util.*
import kotlin.streams.toList

object Utils {
    @Throws(IOException::class)
    fun getLines(stream: InputStream): List<String> {
        BufferedReader(InputStreamReader(stream)).use { reader ->
            return reader.lines().toList()
        }
    }

    fun toWords(input: String): List<String> =
        input.split("\\W+".toRegex())
            .filter { it.isNotEmpty() }
}