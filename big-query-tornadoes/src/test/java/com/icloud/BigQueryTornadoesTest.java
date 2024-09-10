package com.icloud;

import static com.icloud.BigQueryTornadoes.ExtractTornadoesFn;
import static com.icloud.BigQueryTornadoes.FormatCountsFn;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BigQueryTornadoesTest {

  private Pipeline pipeline;

  @BeforeEach
  void setup() {
    this.pipeline = PipelineUtils.create();
  }

  @Test
  void testExtractTornadoes() {
    final TableRow row = new TableRow().set("month", "6").set("tornado", true);

    final PCollection<TableRow> input = pipeline.apply(Create.of(ImmutableList.of(row)));

    final PCollection<Integer> result = input.apply(ParDo.of(new ExtractTornadoesFn()));

    PAssert.that(result).containsInAnyOrder(6);

    pipeline.run();
  }

  @Test
  void testNoTornadoes() {
    final TableRow row = new TableRow().set("month", "6").set("tornado", false);

    final PCollection<TableRow> input = pipeline.apply(Create.of(ImmutableList.of(row)));

    final PCollection<Integer> result = input.apply(ParDo.of(new ExtractTornadoesFn()));

    PAssert.that(result).empty();

    pipeline.run();
  }

  @Test
  void testEmpty() {
    final PCollection<KV<Integer, Long>> inputs =
        pipeline.apply(Create.empty(new TypeDescriptor<KV<Integer, Long>>() {}));

    final PCollection<TableRow> result = inputs.apply(ParDo.of(new FormatCountsFn()));

    PAssert.that(result).empty();

    pipeline.run();
  }
}
