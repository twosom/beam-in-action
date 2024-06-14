package com.icloud;

import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Instant;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static com.icloud.TestUtils.timestampedValue;
import static org.apache.beam.sdk.values.TypeDescriptors.strings;

@RunWith(JUnit4.class)
public class MaxWordLengthTest {

    @Rule
    public TestPipeline p = TestPipeline.create();

    private final Instant baseTime = new Instant(0);

    @Test
    public void testWordLength() {

        final TestStream<String> input = TestStream.create(StringUtf8Coder.of())
                .addElements(timestampedValue("a", baseTime))
                .addElements(timestampedValue("bb", baseTime.plus(10_000)))
                .addElements(timestampedValue("ccc", baseTime.plus(20_000)))
                .addElements(timestampedValue("d", baseTime.plus(30_000)))
                .advanceWatermarkToInfinity();

        final PCollection<String> strings = p.apply(input);
        final PCollection<String> output = MaxWordLength.computeLongestWord(strings)
                .apply(
                        "Convert To KV",
                        MapElements.into(strings())
                                .via(kv -> kv.getTimestamp() + ":" + kv.getValue())
                )
                .apply(LogUtils.of(true));

        PAssert.that(output)
                .containsInAnyOrder(
                        "1970-01-01T00:00:00.000Z:a",
                        "1970-01-01T00:00:10.000Z:bb",
                        "1970-01-01T00:00:20.000Z:ccc",
                        "1970-01-01T00:00:30.000Z:ccc"
                );

        PAssert.that(output)
                .inFinalPane(GlobalWindow.INSTANCE)
                .empty();

        p.run();
    }

}