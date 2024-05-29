package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Repeatedly;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class ProcessingTimeWindow {

    public static void main(String[] args) throws IOException {
        final ClassLoader loader =
                FirstPipeline.class.getClassLoader();

        final String file =
                loader.getResource("lorem.txt").getFile();

        final List<String> lines =
                Files.readAllLines(Paths.get(file), StandardCharsets.UTF_8);

        final Pipeline pipeline = PipelineUtils.create(args);

        final PCollection<String> input =
                pipeline.apply(Create.of(lines));

        final PCollection<String> words =
                input.apply(Tokenize.of())
                        .apply(WithReadDelay.ofProcessingTime(Duration.millis(50)));

        final PCollection<String> windowed = words.apply(
                Window.<String>into(new GlobalWindows())
                        .triggering(
                                Repeatedly.forever(
                                        AfterProcessingTime.pastFirstElementInPane()
                                                .plusDelayOf(Duration.standardSeconds(1))
                                )
                        )
                        .accumulatingFiredPanes()
        );

        final PCollection<Long> result = windowed.apply(Count.globally());

        result.apply(LogUtils.of());

        pipeline.run();
    }


}
