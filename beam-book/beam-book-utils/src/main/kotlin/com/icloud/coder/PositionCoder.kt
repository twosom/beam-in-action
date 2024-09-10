package com.icloud.coder

import com.icloud.model.Position
import org.apache.beam.sdk.coders.CustomCoder
import org.apache.beam.sdk.coders.DoubleCoder
import org.apache.beam.sdk.coders.VarLongCoder
import java.io.InputStream
import java.io.OutputStream

class PositionCoder private constructor() : CustomCoder<Position>() {

    companion object {
        private val DOUBLE_CODER: DoubleCoder by lazy { DoubleCoder.of() }
        private val LONG_CODER: VarLongCoder by lazy { VarLongCoder.of() }

        fun of(): PositionCoder = PositionCoder()
    }

    override fun encode(value: Position, outStream: OutputStream) {
        DOUBLE_CODER.encode(value.latitude, outStream)
        DOUBLE_CODER.encode(value.longitude, outStream)
        LONG_CODER.encode(value.timestamp, outStream)
    }

    override fun decode(inStream: InputStream): Position {
        return Position(
            latitude = DOUBLE_CODER.decode(inStream),
            longitude = DOUBLE_CODER.decode(inStream),
            timestamp = LONG_CODER.decode(inStream)
        )
    }

}