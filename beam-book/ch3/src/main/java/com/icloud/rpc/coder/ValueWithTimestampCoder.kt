package com.icloud.rpc.coder

import com.icloud.rpc.model.ValueWithTimestamp
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.CustomCoder
import org.apache.beam.sdk.coders.InstantCoder
import java.io.InputStream
import java.io.OutputStream

class ValueWithTimestampCoder<T>(
    private val valueCoder: Coder<T>,
) : CustomCoder<ValueWithTimestamp<T>>() {
    companion object {
        private val INSTANT_CODER: InstantCoder = InstantCoder.of()
    }

    override fun encode(
        value: ValueWithTimestamp<T>,
        outStream: OutputStream,
    ) {
        this.valueCoder.encode(value.value, outStream)
        INSTANT_CODER.encode(value.timestamp, outStream)
    }

    override fun decode(
        inStream: InputStream,
    ): ValueWithTimestamp<T> =
        ValueWithTimestamp(
            value = this.valueCoder.decode(inStream),
            timestamp = INSTANT_CODER.decode(inStream)
        )

}