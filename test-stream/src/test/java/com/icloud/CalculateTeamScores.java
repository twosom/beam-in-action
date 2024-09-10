package com.icloud;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;

@VisibleForTesting
public class CalculateTeamScores
    extends PTransform<
        @NonNull PCollection<GameActionInfo>, @NonNull PCollection<KV<String, Integer>>> {
  static final Duration FIVE_MINUTES = Duration.standardMinutes(5);
  static final Duration TEN_MINUTES = Duration.standardMinutes(10);

  private final Duration teamWindowDuration;
  private final Duration allowedLateness;

  CalculateTeamScores(Duration teamWindowDuration, Duration allowedLateness) {
    this.teamWindowDuration = teamWindowDuration;
    this.allowedLateness = allowedLateness;
  }

  @Override
  public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> infos) {
    return infos
        .apply(
            "LeaderboardTeamFixedWindows",
            Window.<GameActionInfo>into(FixedWindows.of(teamWindowDuration))
                // We will get early (speculative) results as well as cumulative
                // processing of late data.
                .triggering(
                    AfterWatermark.pastEndOfWindow()
                        .withEarlyFirings(
                            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(FIVE_MINUTES))
                        .withLateFirings(
                            AfterProcessingTime.pastFirstElementInPane().plusDelayOf(TEN_MINUTES)))
                .withAllowedLateness(allowedLateness)
                .accumulatingFiredPanes())
        // Extract and sum teamname/score pairs from the event data.
        .apply("ExtractTeamScore", new ExtractAndSumScore("team"));
  }
}
