package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class ToStringExample {
  public static void main(String[] args) {
    final Pipeline pipeline = PipelineUtils.create(args);

    PCollection<KV<String, String>> pairs =
        pipeline.apply(
            Create.of(
                KV.of("fall", "apple"),
                KV.of("spring", "strawberry"),
                KV.of("winter", "orange"),
                KV.of("summer", "peach"),
                KV.of("spring", "cherry"),
                KV.of("fall", "pear")));

    pairs.apply(ToString.kvs()).apply(LogUtils.of());

    pipeline.run().waitUntilFinish();
  }
}
