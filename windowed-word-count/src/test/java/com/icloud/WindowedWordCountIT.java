package com.icloud;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.icloud.WindowedWordCount.Options;
import static com.icloud.WindowedWordCount.runWindowedWordCount;
import static com.icloud.WriteOneFilePerWindow.PerWindowFiles;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.beam.sdk.io.FileBasedSink;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.util.*;
import org.apache.hadoop.util.Lists;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WindowedWordCountIT {

  static final int MAX_READ_RETRIES = 4;
  static final Duration DEFAULT_SLEEP_DURATION = Duration.standardSeconds(10L);
  static final FluentBackoff BACK_OFF_FACTORY =
      FluentBackoff.DEFAULT
          .withInitialBackoff(DEFAULT_SLEEP_DURATION)
          .withMaxRetries(MAX_READ_RETRIES);
  private static final String DEFAULT_INPUT = "gs://apache-beam-samples/shakespeare/sonnets.txt";
  @Rule public TestName testName = new TestName();

  static WordCountsMatcher containsWordCounts(SortedMap<String, Long> expectedWordCounts) {
    return new WordCountsMatcher(expectedWordCounts);
  }

  @BeforeClass
  public static void setup() {
    PipelineOptionsFactory.register(TestPipelineOptions.class);
  }

  @Test
  public void testWindowedWordCountInBatchDynamicSharding() throws Exception {
    final WindowedWordCountITOptions options = batchOptions();
    options.setNumShards(null);
    testWindowedWordCountPipeline(options);
  }

  private void testWindowedWordCountPipeline(WindowedWordCountITOptions options) throws Exception {
    final ResourceId output = FileBasedSink.convertToFileResourceIfPossible(options.getOutput());
    final PerWindowFiles fileNamePolicy = new PerWindowFiles(output);

    final List<ShardedFile> expectedOutputFiles = Lists.newArrayListWithCapacity(6);

    for (int startMinute : ImmutableList.of(0, 10, 20, 30, 40, 50)) {
      final Instant windowStart =
          new Instant(options.getMinTimestampMillis()).plus(Duration.standardMinutes(startMinute));

      final String filePrefix =
          fileNamePolicy.filenamePrefixForWindow(
              new IntervalWindow(windowStart, windowStart.plus(Duration.standardMinutes(10))));

      expectedOutputFiles.add(
          new NumberedShardedFile(
              output
                      .getCurrentDirectory()
                      .resolve(filePrefix, ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
                  + "*"));
    }

    final ShardedFile inputFile =
        new ExplicitShardedFile(Collections.singleton(options.getInputFile()));

    final SortedMap<String, Long> expectedWordCounts = new TreeMap<>();

    for (String line :
        inputFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff())) {
      final String[] words = line.split(ExampleUtils.TOKENIZER_PATTERN, -1);
      for (String word : words) {
        if (!word.isEmpty()) {
          expectedWordCounts.put(word, firstNonNull(expectedWordCounts.get(word), 0L) + 1L);
        }
      }
    }

    runWindowedWordCount(options);

    assertThat(expectedOutputFiles, containsWordCounts(expectedWordCounts));
  }

  private WindowedWordCountITOptions batchOptions() {
    final WindowedWordCountITOptions options = defaultOptions();
    options.setStreaming(false);
    return options;
  }

  private WindowedWordCountITOptions defaultOptions() {
    final WindowedWordCountITOptions options =
        TestPipeline.testingPipelineOptions().as(WindowedWordCountITOptions.class);

    options.setInputFile(DEFAULT_INPUT);
    options.setTestTimeoutSeconds(1200L);

    options.setMinTimestampMillis(0L);
    options.setMaxTimestampMillis(Duration.standardHours(1).getMillis());
    options.setWindowSize(10);

    options.setOutput(
        FileSystems.matchNewResource("/tmp", true)
            .resolve(
                String.format(
                    "WindowedWordCountIT.%s-%tFT%<tH:%<tM:%<tS.%<tL+%s",
                    testName.getMethodName(), new Date(), ThreadLocalRandom.current().nextInt()),
                ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("output", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
            .resolve("results", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
            .toString());
    return options;
  }

  public interface WindowedWordCountITOptions
      extends Options, TestPipelineOptions, StreamingOptions {}

  private static class WordCountsMatcher extends TypeSafeMatcher<List<ShardedFile>> {

    private final SortedMap<String, Long> expectedWordCounts;
    private SortedMap<String, Long> actualCounts;

    private WordCountsMatcher(SortedMap<String, Long> expectedWordCounts) {
      this.expectedWordCounts = expectedWordCounts;
    }

    @Override
    protected boolean matchesSafely(List<ShardedFile> outputFiles) {
      try {
        final List<String> outputLines = new ArrayList<>();
        for (ShardedFile outputFile : outputFiles) {
          outputLines.addAll(
              outputFile.readFilesWithRetries(Sleeper.DEFAULT, BACK_OFF_FACTORY.backoff()));
        }

        this.actualCounts = new TreeMap<>();
        for (String line : outputLines) {
          final String[] splits = line.split(": ", -1);
          final String word = splits[0];
          final long count = Long.parseLong(splits[1]);
          this.actualCounts.merge(word, count, Long::sum);
        }

        return this.actualCounts.equals(this.expectedWordCounts);
      } catch (Exception e) {
        throw new RuntimeException(
            String.format("Failed to read from sharded output: %s due to exception", outputFiles),
            e);
      }
    }

    @Override
    public void describeTo(Description description) {
      equalTo(this.expectedWordCounts).describeTo(description);
    }

    @Override
    protected void describeMismatchSafely(List<ShardedFile> item, Description description) {
      equalTo(this.expectedWordCounts).describeMismatch(this.actualCounts, description);
    }
  }
}
