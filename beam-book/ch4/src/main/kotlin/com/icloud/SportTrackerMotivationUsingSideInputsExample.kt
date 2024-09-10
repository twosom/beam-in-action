package com.icloud

import com.icloud.extensions.kv
import com.icloud.extensions.kvs
import com.icloud.extensions.minutes
import com.icloud.model.Position
import org.apache.beam.sdk.transforms.*
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.SlidingWindows
import org.apache.beam.sdk.transforms.windowing.Window
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.TypeDescriptors.booleans
import org.apache.beam.sdk.values.TypeDescriptors.strings
import java.util.*


class SportTrackerMotivationUsingSideInputs
    : PTransform<PCollection<KV<String, Position>>, PCollection<KV<String, Boolean>>>() {

    override fun expand(
        input: PCollection<KV<String, Position>>,
    ): PCollection<KV<String, Boolean>> {
        val metrics = input.apply("computeMetric", ToMetric())

        val longRunningAverageView = metrics
            .apply(Window.into(SlidingWindows.of(5.minutes()).every(1.minutes())))
            .apply("longAverage", ComputeAverage())
            .apply(Window.into(FixedWindows.of(1.minutes())))
            .apply("longMap", View.asMap())


        return metrics
            .apply(Window.into(FixedWindows.of(1.minutes())))
            .apply("shortAverage", ComputeAverage())
            .apply(
                FlatMapElements.into(strings() kvs booleans())
                    .via(Contextful.fn({ elem, c ->
                        val longPace: Double =
                            c.sideInput(longRunningAverageView)[elem.key] ?: 0.0
                        if (longPace > .0) {
                            val ratio = elem.value / longPace
                            if (ratio < 0.0 || ratio > 1.1) {
                                return@fn Collections.singletonList(elem.key kv (ratio > 1.0))
                            }
                        }
                        return@fn Collections.emptyList()
                    }, Requirements.requiresSideInputs(longRunningAverageView)))
            )
    }

}

object SportTrackerMotivationUsingSideInputsExample {
    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, CommonKafkaOptions::class.java)
        pipeline.apply(
            "Read Records",
            ReadPositionFromKafka(
                bootstrapServer = options.bootstrapServer,
                inputTopic = options.inputTopic
            )
        )
            .apply("computeTrackNotifications", SportTrackerMotivationUsingSideInputs())
            .apply(
                "createUserNotification", WriteNotificationsToKafka(
                    bootstrapServer = options.bootstrapServer,
                    outputTopic = options.outputTopic
                )
            )

        pipeline.run()
    }
}