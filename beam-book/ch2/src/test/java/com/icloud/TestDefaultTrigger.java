package com.icloud;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.TimestampCombiner;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.icloud.TestUtils.timestampedValue;

@RunWith(JUnit4.class)
public class TestDefaultTrigger {

    @Rule
    public TestPipeline p = TestPipeline.create();

    private final Instant baseTime = new Instant(0);

    private static final Duration WINDOW_DURATION =
            Duration.standardSeconds(10L);

    private static final Duration ALLOWED_LATENESS =
            Duration.standardSeconds(5L);

    @Test
    public void test_default_trigger() {
        final TestStream<String> testStream = TestStream.create(StringUtf8Coder.of())
                .advanceWatermarkTo(baseTime)
                .addElements(
                        timestampedValue("hello", baseTime.plus(Duration.millis(1)))
                )
                .advanceWatermarkTo(baseTime.plus(WINDOW_DURATION).plus(ALLOWED_LATENESS).minus(Duration.millis(1)))
                .addElements(
                        // late data
                        timestampedValue("world", baseTime.plus(Duration.standardSeconds(3L)))
                )
                .advanceWatermarkToInfinity();

        final PCollection<String> result = p.apply(testStream)
                .apply(
                        Window.<String>into(FixedWindows.of(WINDOW_DURATION))
                                // with default trigger
                                .withAllowedLateness(ALLOWED_LATENESS)
                                .accumulatingFiredPanes()
                                .withTimestampCombiner(TimestampCombiner.END_OF_WINDOW)
                                .withOnTimeBehavior(Window.OnTimeBehavior.FIRE_IF_NON_EMPTY)
                )
                .apply(Count.perElement())
                .apply(Reify.timestamps())
                .apply(MapElements.into(TypeDescriptors.strings())
                        .via(e -> e.getTimestamp() + ":" + e.getValue()))
                .apply(LogUtils.of(true));

        PAssert.that(result)
                .containsInAnyOrder(
                        "1970-01-01T00:00:09.999Z:KV{hello, 1}",
                        "1970-01-01T00:00:09.999Z:KV{world, 1}"
                );
        p.run();
    }

}
