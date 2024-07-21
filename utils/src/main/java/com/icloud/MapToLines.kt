package com.icloud

import com.icloud.extensions.kVia
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

class MapToLines<K, T>(
    private val includeKey: Boolean,
) : PTransform<PCollection<KafkaRecord<K, T>>, PCollection<String>>() {

    companion object {
        @JvmStatic
        fun <K, V> of(): MapToLines<K, V> = MapToLines(true)

        @JvmStatic
        fun <K, V> ofValuesOnly(): MapToLines<K, V> = MapToLines(false)
    }

    override fun expand(
        input: PCollection<KafkaRecord<K, T>>,
    ): PCollection<String> {
        return input.apply(
            MapElements.into(TypeDescriptors.strings())
                .kVia { r ->
                    if (includeKey) ifNotNull(r.kv.key) + " " + ifNotNull(r.kv.value)
                    else ifNotNull(r.kv.value)
                }
        )
    }

    private fun <T> ifNotNull(value: T?): String =
        value?.toString() ?: ""
}