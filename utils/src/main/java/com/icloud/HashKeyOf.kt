package com.icloud

import com.icloud.extensions.kVia
import com.icloud.extensions.kv
import com.icloud.extensions.kvs
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors

class HashKeyOf<T> private constructor(
    private val keySize: Int,
) : PTransform<PCollection<T>, PCollection<KV<Int, T>>>() {
    companion object {
        @JvmStatic
        fun <T> of(keySize: Int) = HashKeyOf<T>(keySize)
    }

    override fun expand(
        input: PCollection<T>,
    ): PCollection<KV<Int, T>> {
        val typeDescriptor = input.typeDescriptor ?: throw RuntimeException("unknown TypeDescriptor<T>")
        return input.apply(MapElements.into(TypeDescriptors.integers() kvs typeDescriptor)
            .kVia { (it.hashCode() and Int.MAX_VALUE % this.keySize) kv it })
    }

}