package com.icloud;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

import static com.icloud.CalculateTeamScores.TEN_MINUTES;

@VisibleForTesting
public class CalculateUserScores
        extends PTransform<@NonNull PCollection<GameActionInfo>, @NonNull PCollection<KV<String, Integer>>> {
    private final Duration allowedLateness;

    CalculateUserScores(Duration allowedLateness) {
        this.allowedLateness = allowedLateness;
    }

    @Override
    public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> input) {
        return input
                .apply(
                        "LeaderboardUserGlobalWindow",
                        Window.<GameActionInfo>into(new GlobalWindows())
                                // Get periodic results every ten minutes.
                                .triggering(
                                        Repeatedly.forever(
                                                AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TEN_MINUTES)))
                                .accumulatingFiredPanes()
                                .withAllowedLateness(allowedLateness))
                // Extract and sum username/score pairs from the event data.
                .apply("ExtractUserScore", new ExtractAndSumScore("user"));
    }
}