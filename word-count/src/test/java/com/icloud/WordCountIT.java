package com.icloud;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.TestPipelineOptions;
import org.apache.beam.sdk.util.NumberedShardedFile;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Date;

import static com.icloud.WordCount.WordCountOptions;
import static com.icloud.WordCount.runWordCount;
import static org.apache.beam.sdk.testing.FileChecksumMatcher.fileContentsHaveChecksum;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(JUnit4.class)
public class WordCountIT {
    private static final String DEFAULT_INPUT =
            "gs://apache-beam-samples/shakespeare/winterstale-personae";
    private static final String DEFAULT_OUTPUT_CHECKSUM = "ebf895e7324e8a3edc72e7bcc96fa2ba7f690def";

    public interface WordCountITOptions
            extends TestPipelineOptions, WordCountOptions {
    }

    @BeforeClass
    public static void setUp() {
        PipelineOptionsFactory.register(TestPipelineOptions.class);
    }

    @Test
    public void testE2WordCount() {
        final WordCountITOptions options = TestPipeline.testingPipelineOptions().as(WordCountITOptions.class);

        options.setInputFile(DEFAULT_INPUT);
        options.setOutput(
                FileSystems.matchNewResource("/tmp", true)
                        .resolve(
                                String.format("WordCountIT-%tF-%<tH-%<tM-%<tS-%<tL", new Date()),
                                ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY
                        )
                        .resolve("output", ResolveOptions.StandardResolveOptions.RESOLVE_DIRECTORY)
                        .resolve("results", ResolveOptions.StandardResolveOptions.RESOLVE_FILE)
                        .toString()
        );
        runWordCount(options);

        assertThat(
                new NumberedShardedFile(options.getOutput() + "*-of-*"),
                fileContentsHaveChecksum(DEFAULT_OUTPUT_CHECKSUM));
    }
}
