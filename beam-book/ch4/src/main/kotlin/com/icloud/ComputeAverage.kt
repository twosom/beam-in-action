package com.icloud

import com.icloud.model.Metric
import org.apache.beam.sdk.coders.*
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Combine.CombineFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import java.io.InputStream
import java.io.OutputStream

class ComputeAverage
    : PTransform<PCollection<KV<String, Metric>>, PCollection<KV<String, Double>>>() {

    override fun expand(
        input: PCollection<KV<String, Metric>>,
    ): PCollection<KV<String, Double>> =
        input.apply(Combine.perKey(AveragePaceFn()))


    data class MetricAccumulator(
        val totalDuration: Long,
        val totalDistance: Double,
    ) {
        operator fun plus(
            that: Metric,
        ): MetricAccumulator =
            MetricAccumulator(
                totalDistance = this.totalDistance + that.length,
                totalDuration = this.totalDuration + that.duration
            )

        operator fun plus(
            that: MetricAccumulator,
        ): MetricAccumulator =
            MetricAccumulator(
                totalDuration = this.totalDuration + that.totalDuration,
                totalDistance = this.totalDistance + that.totalDistance
            )

        fun output(): Double =
            (this.totalDistance * 1_000) / this.totalDuration
    }

    class MetricAccumulatorCoder private constructor() : CustomCoder<MetricAccumulator>() {
        companion object {
            private val DOUBLE_CODER = DoubleCoder.of()
            private val LONG_CODER = VarLongCoder.of()

            @JvmStatic
            fun of(): MetricAccumulatorCoder = MetricAccumulatorCoder()
        }

        override fun encode(
            value: MetricAccumulator,
            outStream: OutputStream,
        ) =
            LONG_CODER.encode(value.totalDuration, outStream)
                .also { DOUBLE_CODER.encode(value.totalDistance, outStream) }

        override fun decode(
            inStream: InputStream,
        ): MetricAccumulator =
            MetricAccumulator(
                totalDuration = LONG_CODER.decode(inStream),
                totalDistance = DOUBLE_CODER.decode(inStream)
            )

    }


    private class AveragePaceFn
        : CombineFn<Metric, MetricAccumulator, Double>() {

        override fun createAccumulator(): MetricAccumulator = MetricAccumulator(0, .0)

        override fun addInput(
            accum: MetricAccumulator,
            input: Metric,
        ): MetricAccumulator = accum + input

        override fun mergeAccumulators(
            accumulators: Iterable<MetricAccumulator>,
        ): MetricAccumulator {
            var merged: MetricAccumulator? = null
            for (accum in accumulators) {
                if (merged == null) {
                    merged = accum
                } else {
                    merged += accum
                }
            }
            check(merged != null) { "merged value must not be null" }
            return merged
        }

        override fun extractOutput(
            accum: MetricAccumulator,
        ): Double = accum.output()

        override fun getAccumulatorCoder(
            registry: CoderRegistry,
            inputCoder: Coder<Metric>,
        ): Coder<MetricAccumulator> = MetricAccumulatorCoder.of()
    }


}