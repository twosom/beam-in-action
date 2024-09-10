package com.icloud;

import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

public class WindowExample {
    public static void main(String[] args) {
        final Pipeline pipeline = PipelineUtils.create(args);

        final List<String> inputData = Arrays.asList("foo", "bar", "foo", "foo");
        final List<Long> timestamps = Arrays.asList(
                Duration.standardSeconds(15).getMillis(),
                Duration.standardSeconds(30).getMillis(),
                Duration.standardSeconds(45).getMillis(),
                Duration.standardSeconds(90).getMillis()
        );

        final PCollection<String> items =
                pipeline.apply(Create.timestamped(inputData, timestamps));

        final PCollection<String> windowedItem =
                items.apply(Window.into(FixedWindows.of(Duration.standardMinutes(1))));

        final PCollection<KV<String, Long>> windowedCounts = windowedItem.apply(Count.perElement());

        windowedCounts.apply(Reify.windows())
                .apply(LogUtils.of());

        pipeline.run().waitUntilFinish();

    }
}
