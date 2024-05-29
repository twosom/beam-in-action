package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

public class FirstPipeline {
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
                input.apply(Tokenize.of());

        words.apply(Count.perElement())
                .apply(LogUtils.of());

        pipeline.run()
                .waitUntilFinish();
    }
}
