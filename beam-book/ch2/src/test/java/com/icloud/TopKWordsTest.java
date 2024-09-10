package com.icloud;

import static com.icloud.TestUtils.timestampedValue;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@SuppressWarnings("unchecked")
@RunWith(JUnit4.class)
public class TopKWordsTest {

  @Rule public final TestPipeline p = TestPipeline.create();

  @Test
  public void test() {
    final Duration windowDuration = Duration.standardSeconds(10);
    final Instant now = Instant.now();
    final Instant startOfTenSecondWindow = now.minus(now.getMillis() % windowDuration.getMillis());

    final TestStream<String> lines = createInput(startOfTenSecondWindow);
    final PCollection<String> input = p.apply(lines);

    final PCollection<KV<String, Long>> output =
        TopKWords.countWordsInFixedWindows(input, windowDuration, 3);

    final IntervalWindow firstWindow =
        new IntervalWindow(startOfTenSecondWindow, startOfTenSecondWindow.plus(windowDuration));

    final IntervalWindow secondWindow =
        new IntervalWindow(
            startOfTenSecondWindow.plus(windowDuration),
            startOfTenSecondWindow.plus(windowDuration).plus(windowDuration));

    PAssert.that(output)
        .containsInAnyOrder(
            KV.of("line", 3L),
            KV.of("first", 3L),
            KV.of("the", 3L),
            KV.of("line", 3L),
            KV.of("in", 2L),
            KV.of("window", 2L));

    PAssert.that(output)
        .inWindow(firstWindow)
        .containsInAnyOrder(KV.of("first", 3L), KV.of("line", 3L), KV.of("the", 3L));

    PAssert.that(output)
        .inWindow(secondWindow)
        .containsInAnyOrder(KV.of("line", 3L), KV.of("in", 2L), KV.of("window", 2L));

    p.run();
  }

  private TestStream<String> createInput(Instant baseTime) {
    return TestStream.create(StringUtf8Coder.of())
        .addElements(
            timestampedValue("This is the first line", baseTime),
            timestampedValue("This is second line in the first window", baseTime.plus(1_000)),
            timestampedValue("Last line in the first window", baseTime.plus(2_000)),
            timestampedValue(
                "This is another line, but in different window.", baseTime.plus(10_000)),
            timestampedValue(
                "Last line, in the same window as previous line.", baseTime.plus(11_000)))
        .advanceWatermarkToInfinity();
  }
}
