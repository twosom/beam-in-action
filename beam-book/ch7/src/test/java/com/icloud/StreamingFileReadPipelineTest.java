package com.icloud;

import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.icloud.io.DirectoryWatch;
import com.icloud.io.FileRead;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
class StreamingFileReadPipelineTest {

  Pipeline p = Pipeline.create();

  @TempDir Path tempDir;

  @RepeatedTest(500)
  void testDirectoryWatch() throws IOException, InterruptedException, ExecutionException {
    final PCollection<String> files =
        p.apply(
            DirectoryWatch.read(this.tempDir.toString())
                .withWatermarkFn(
                    kv -> {
                      if (kv.getKey().endsWith("b")) {
                        return BoundedWindow.TIMESTAMP_MAX_VALUE;
                      }
                      return kv.getValue();
                    }));

    PAssert.that(files)
        .containsInAnyOrder(
            new File(tempDir.toFile(), "a").toString(), new File(tempDir.toFile(), "b").toString());

    final CompletableFuture<PipelineResult> future = CompletableFuture.supplyAsync(p::run);
    createFileInDirectory(tempDir, "a");
    TimeUnit.SECONDS.sleep(1);
    createFileInDirectory(tempDir, "b");
    final PipelineResult result = future.get();
    assertThat(result.getState(), is(PipelineResult.State.DONE));
  }

  @Test
  @SuppressWarnings("unchecked")
  void testFileRead() throws IOException {
    List<String> tempFiles =
        Arrays.asList(
            newTempFile(tempDir, 100).toString(), // 0
            newTempFile(tempDir, 900).toString(), // 0, 1, 2, 3, 4, 5, 6, 7, 8
            newTempFile(tempDir, 10000).toString(), // 10x0, 10x1, 10x2, 10x3, ..
            newTempFile(tempDir, 100000).toString()); // 100x0, 100x1, 100x2, 100x3,

    final PCollection<KV<String, Long>> result =
        p.apply(Create.of(tempFiles))
            .apply("fileRead", FileRead.read())
            .apply(Count.perElement())
            .apply(
                MapElements.into(kvs(strings(), longs()))
                    .via(kv -> KV.of(kv.getKey().substring(0, 1), kv.getValue())));

    PAssert.that(result)
        .containsInAnyOrder(
            KV.of("0", 112L),
            KV.of("1", 111L),
            KV.of("2", 111L),
            KV.of("3", 111L),
            KV.of("4", 111L),
            KV.of("5", 111L),
            KV.of("6", 111L),
            KV.of("7", 111L),
            KV.of("8", 111L),
            KV.of("9", 110L));

    p.run();
  }

  private void createFileInDirectory(Path tempDir, String file) throws IOException {
    Files.createFile(new File(tempDir.toFile(), file).toPath());
  }

  private File newTempFile(Path tempDir, int length) throws IOException {
    Path tempFile = Files.createTempFile(tempDir, "", ".tmp");
    int size = 0;
    int lineNum = 0;
    try (FileOutputStream fos = new FileOutputStream(tempFile.toFile())) {
      while (size < length) {
        String line =
            asStringOfLength(Character.forDigit(lineNum++ % 10, 10), Math.min(99, length - size));
        fos.write(line.getBytes(StandardCharsets.UTF_8));
        size += line.length();
        if (size < length) {
          fos.write('\n');
          size += 1;
        }
      }
    }
    return tempFile.toFile();
  }

  private String asStringOfLength(char character, int length) {
    StringBuffer builder = new StringBuffer(length);
    for (int i = 0; i < length; i++) {
      builder.append(character);
    }
    return builder.toString();
  }
}
