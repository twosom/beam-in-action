package com.icloud;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
@SuppressWarnings("unchecked")
public class PipelineTest {

  private static final Duration TEAM_WINDOW_DURATION = Duration.standardMinutes(20);

  private static final Duration ALLOWED_LATENESS = Duration.standardHours(1);

  private final Instant baseTime = new Instant(0L);

  @Rule public TestPipeline p = TestPipeline.create();

  @Test
  public void test_pipeline_is_not_null() {
    assertNotNull(p);
  }

  @Test
  public void everything_arrives_on_time() {
    final TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                gameActionInfo("sky", "blue", 12, Duration.ZERO),
                gameActionInfo("navy", "blue", 3, Duration.ZERO),
                gameActionInfo("navy", "blue", 3, Duration.standardMinutes(3L)))
            .advanceWatermarkTo(
                baseTime.plus(TEAM_WINDOW_DURATION).plus(Duration.standardMinutes(1L)))
            .advanceWatermarkToInfinity();

    final PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    final IntervalWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);

    PAssert.that(teamScores).inWindow(window).containsInAnyOrder(KV.of("blue", 18));

    p.run();
  }

  @Test
  public void some_elements_are_late_but_arrive_before_the_end_of_the_window() {
    final TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                gameActionInfo("sky", "blue", 3, Duration.ZERO),
                gameActionInfo("navy", "blue", 3, Duration.standardMinutes(3L)))
            .advanceWatermarkTo(
                baseTime.plus(TEAM_WINDOW_DURATION).minus(Duration.standardMinutes(1L)))
            .addElements(gameActionInfo("sky", "blue", 12, Duration.ZERO))
            .advanceWatermarkToInfinity();

    final PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    final IntervalWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);

    PAssert.that(teamScores).inOnTimePane(window).containsInAnyOrder(KV.of("blue", 18));

    p.run();
  }

  @Test
  public void elements_are_late_and_arrive_after_the_end_of_the_window() {
    final TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                gameActionInfo("sky", "blue", 3, Duration.ZERO),
                gameActionInfo("navy", "blue", 3, Duration.standardMinutes(3L)))

            // Move the watermark up to "near" the end of the window + allowed lateness
            .advanceWatermarkTo(
                baseTime
                    .plus(TEAM_WINDOW_DURATION)
                    .plus(ALLOWED_LATENESS)
                    .minus(Duration.standardMinutes(1L)))
            .addElements(gameActionInfo("sky", "blue", 12, Duration.ZERO))
            .advanceWatermarkToInfinity();

    final PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS))
            .apply(LogUtils.of());

    final IntervalWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);

    PAssert.that(teamScores).inOnTimePane(window).containsInAnyOrder(KV.of("blue", 6));

    PAssert.that(teamScores).inFinalPane(window).containsInAnyOrder(KV.of("blue", 18));

    p.run();
  }

  @Test
  public void elements_are_late_and_after_the_end_of_the_window_plus_the_allowed_lateness() {
    final TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                gameActionInfo("sky", "blue", 3, Duration.ZERO),
                gameActionInfo("navy", "blue", 3, Duration.standardMinutes(3L)))
            .advanceWatermarkTo(
                baseTime
                    .plus(TEAM_WINDOW_DURATION)
                    .plus(ALLOWED_LATENESS)
                    .plus(Duration.standardMinutes(1)))
            .addElements(
                gameActionInfo(
                    "sky",
                    "blue",
                    12,
                    Duration.millis(
                        baseTime
                            .plus(TEAM_WINDOW_DURATION)
                            .minus(Duration.standardMinutes(1L))
                            .getMillis())))
            .advanceWatermarkToInfinity();

    final PCollection<KV<String, Integer>> teamScores =
        p.apply(createEvents)
            .apply(new CalculateTeamScores(TEAM_WINDOW_DURATION, ALLOWED_LATENESS));

    final IntervalWindow window = new IntervalWindow(baseTime, TEAM_WINDOW_DURATION);
    PAssert.that(teamScores).inWindow(window).containsInAnyOrder(KV.of("blue", 6));

    p.run();
  }

  @Test
  public void elements_arrive_before_the_end_of_the_window_and_some_processing_time_passes() {
    final TestStream<GameActionInfo> createEvents =
        TestStream.create(AvroCoder.of(GameActionInfo.class))
            .advanceWatermarkTo(baseTime)
            .addElements(
                gameActionInfo("scarlet", "red", 3, Duration.ZERO),
                gameActionInfo("scarlet", "red", 2, Duration.standardMinutes(1L)))
            .advanceProcessingTime(Duration.standardMinutes(12L))
            .addElements(
                gameActionInfo("oxblood", "red", 2, Duration.standardSeconds(22L)),
                gameActionInfo("scarlet", "red", 4, Duration.standardMinutes(2L)))
            .advanceProcessingTime(Duration.standardMinutes(15L))
            .advanceWatermarkToInfinity();

    final PCollection<KV<String, Integer>> userScores =
        p.apply(createEvents)
            .apply(new CalculateUserScores(ALLOWED_LATENESS))
            .apply(LogUtils.of(true));

    PAssert.that(userScores)
        .inEarlyGlobalWindowPanes()
        .containsInAnyOrder(KV.of("scarlet", 5), KV.of("scarlet", 9), KV.of("oxblood", 2));

    p.run();
  }

  private TimestampedValue<GameActionInfo> gameActionInfo(
      String name, String team, int score, Duration timestamp) {
    return TimestampedValue.of(
        new GameActionInfo(name, team, score, timestamp.getMillis()), baseTime.plus(timestamp));
  }
}
