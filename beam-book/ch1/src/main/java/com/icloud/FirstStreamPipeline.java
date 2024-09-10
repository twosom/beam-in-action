package com.icloud;

import com.google.common.io.Files;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.TestStream;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.windowing.AfterWatermark;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

public class FirstStreamPipeline {

  public static void main(String[] args) throws IOException {
    final ClassLoader loader = FirstStreamPipeline.class.getClassLoader();

    final String file = loader.getResource("lorem.txt").getFile();

    final List<String> lines = Files.readLines(new File(file), StandardCharsets.UTF_8);

    TestStream.Builder<String> builder = TestStream.create(StringUtf8Coder.of());

    final Instant now = Instant.now();

    final List<TimestampedValue<String>> timestamped =
        IntStream.range(0, lines.size())
            .mapToObj(i -> TimestampedValue.of(lines.get(i), now.plus(i)))
            .collect(Collectors.toList());

    for (TimestampedValue<String> value : timestamped) {
      builder = builder.addElements(value);
    }

    final Pipeline pipeline = PipelineUtils.create(args);

    pipeline
        .apply(builder.advanceWatermarkToInfinity())
        .apply(Tokenize.of())
        .apply(
            Window.<String>into(new GlobalWindows())
                .discardingFiredPanes()
                .triggering(AfterWatermark.pastEndOfWindow()))
        .apply(Count.perElement())
        .apply(LogUtils.of());

    pipeline.run();
  }
}
