package com.icloud.coder

import com.icloud.model.Metric
import org.apache.beam.sdk.coders.CustomCoder
import org.apache.beam.sdk.util.VarInt
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.InputStream
import java.io.OutputStream

class MetricCoder : CustomCoder<Metric>() {
    override fun encode(value: Metric, outStream: OutputStream) {
        val dos = DataOutputStream(outStream)
        dos.writeDouble(value.length)
        VarInt.encode(value.time, dos)
    }

    override fun decode(inStream: InputStream): Metric {
        val dis = DataInputStream(inStream)
        return Metric(dis.readDouble(), VarInt.decodeLong(dis))
    }
}