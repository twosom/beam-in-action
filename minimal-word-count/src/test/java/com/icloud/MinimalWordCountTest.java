package com.icloud;

import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.extensions.gcp.options.GcsOptions;
import org.apache.beam.sdk.extensions.gcp.util.GcsUtil;
import org.apache.beam.sdk.extensions.gcp.util.gcsfs.GcsPath;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(JUnit4.class)
public class MinimalWordCountTest {

    @Rule
    public TestPipeline p = TestPipeline.create().enableAbandonedNodeEnforcement(false);

    @Test
    public void testMinimalWordCount() throws IOException {
        p.getOptions().as(GcsOptions.class).setGcsUtil(this.buildMockGcsUtil());

        p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/*"))
                .apply(FlatMapElements.into(strings())
                        .via(word -> Arrays.asList(word.split("[^a-zA-Z']+"))))
                .apply(Filter.by(word -> !word.isEmpty()))
                .apply(Count.perElement())
                .apply(MapElements.into(strings())
                        .via((KV<String, Long> wordCount) ->
                                wordCount.getKey() + ": " + wordCount.getValue()
                        )
                )
                .apply(TextIO.write().to("gs://your-output-bucket/and-output-prefix"))
        ;
    }

    private GcsUtil buildMockGcsUtil() throws IOException {
        final GcsUtil mockGcsUtil = mock(GcsUtil.class);

        when(mockGcsUtil.open(any(GcsPath.class)))
                .then(invocation ->
                        FileChannel.open(
                                Files.createTempFile("channel-", ".tmp"),
                                StandardOpenOption.CREATE,
                                StandardOpenOption.DELETE_ON_CLOSE
                        )
                );

        when(mockGcsUtil.expand(any(GcsPath.class)))
                .then(invocation -> ImmutableList.of((GcsPath) invocation.getArguments()[0]));

        return mockGcsUtil;
    }
}
