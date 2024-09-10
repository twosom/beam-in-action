package com.icloud;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.Sum;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ExtractAndSumScore
    extends PTransform<
        @NonNull PCollection<GameActionInfo>, @NonNull PCollection<KV<String, Integer>>> {

  private final String field;

  ExtractAndSumScore(String field) {
    this.field = field;
  }

  @Override
  public PCollection<KV<String, Integer>> expand(PCollection<GameActionInfo> gameInfo) {

    return gameInfo
        .apply(
            MapElements.into(kvs(strings(), integers()))
                .via(gInfo -> KV.of(gInfo.getKey(field), gInfo.getScore())))
        .apply(Sum.integersPerKey());
  }
}
