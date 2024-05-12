package com.icloud;

import com.google.common.collect.Streams;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.joda.time.Duration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class WindowExampleTest {
    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testWindowExample() {
        final List<String> input = Arrays.asList("foo", "bar", "foo", "foo");
        final List<Long> timestamps = Arrays.asList(
                Duration.standardSeconds(15).getMillis(),
                Duration.standardSeconds(30).getMillis(),
                Duration.standardSeconds(45).getMillis(),
                Duration.standardSeconds(90).getMillis()
        );

        final PCollection<ValueInSingleWindow<KV<String, Long>>> windowedCounts = pipeline.apply(Create.timestamped(input, timestamps))
                .apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))))
                .apply(Count.perElement())
                .apply(Reify.windows());

        PAssert.that(windowedCounts)
                .satisfies(i -> {
                    final long windowCount = Streams.stream(i)
                            .count();
                    assertEquals(windowCount, 3);

                    return null;
                });
        pipeline.run();
    }

}