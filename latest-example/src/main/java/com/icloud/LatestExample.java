package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class LatestExample {


    public static void main(String[] args) {
        final Pipeline pipeline = PipelineUtils.create(args);

        pipeline.apply(GenerateSequence.from(0)
                        .withRate(10, Duration.standardSeconds(30))
                        .withTimestampFn(e -> Instant.now())
                )
                .apply(Window.<Long>configure()
                        .triggering(
                                Repeatedly.forever(AfterProcessingTime.pastFirstElementInPane()
                                        .plusDelayOf(Duration.standardSeconds(10)))
                        )
                        .withAllowedLateness(Duration.standardSeconds(10))
                        .discardingFiredPanes()
                )
                .apply(Latest.globally())
                .apply(LogUtils.of());

        pipeline.run().waitUntilFinish();
    }
}
