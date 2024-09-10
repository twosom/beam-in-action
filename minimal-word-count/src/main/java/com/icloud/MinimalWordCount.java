package com.icloud;

import java.util.Arrays;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TypeDescriptors;

public class MinimalWordCount {
  public static void main(String[] args) {
    final Pipeline p = PipelineUtils.create(args);

    p.apply(TextIO.read().from("gs://apache-beam-samples/shakespeare/kinglear.txt"))
        .apply(
            FlatMapElements.into(TypeDescriptors.strings())
                .via(e -> Arrays.asList(e.split("[^\\p{L}]+"))))
        .apply(Filter.by(word -> !word.isEmpty()))
        .apply(Count.perElement())
        .apply(
            MapElements.into(TypeDescriptors.strings())
                .via(
                    (KV<String, Long> wordCount) ->
                        wordCount.getKey() + ": " + wordCount.getValue()))
        .apply(LogUtils.of());

    p.run().waitUntilFinish();
  }
}
