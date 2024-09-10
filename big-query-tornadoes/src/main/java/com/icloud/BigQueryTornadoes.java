package com.icloud;

import static org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.TypedRead;

import com.google.api.services.bigquery.model.TableRow;
import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

public class BigQueryTornadoes {

  private static final String WEATHER_SAMPLES_TABLE =
      "apache-beam-testing.samples.weather_stations";

  public static void main(String[] args) {
    final Pipeline pipeline = PipelineUtils.create(args, Options.class);
    final Options options = pipeline.getOptions().as(Options.class);

    final PCollection<TableRow> input =
        pipeline.apply(
            "ReadFromBigQuery",
            BigQueryIO.readTableRows()
                .from(options.getInput())
                .withMethod(options.getReadMethod()));

    input.apply(new CountTornadoes()).apply(LogUtils.of());

    pipeline.run().waitUntilFinish();
  }

  public interface Options extends PipelineOptions {
    @Description("Table to read from, specified as <project_id>:<dataset_id>.<table_id>")
    @Default.String(WEATHER_SAMPLES_TABLE)
    String getInput();

    void setInput(String value);

    @Description("SQL Query to read from, will be used if Input is not set.")
    @Default.String("")
    String getInputQuery();

    void setInputQuery(String value);

    @Description("Read method to use to read from BigQuery")
    @Default.Enum("DIRECT_READ")
    TypedRead.Method getReadMethod();

    void setReadMethod(TypedRead.Method value);
  }

  static class CountTornadoes
      extends PTransform<@NonNull PCollection<TableRow>, @NonNull PCollection<TableRow>> {

    @Override
    public PCollection<TableRow> expand(PCollection<TableRow> input) {
      return input
          .apply(ParDo.of(new ExtractTornadoesFn()))
          .apply(Count.perElement())
          .apply(ParDo.of(new FormatCountsFn()));
    }
  }

  @VisibleForTesting
  static class ExtractTornadoesFn extends DoFn<TableRow, Integer> {
    @ProcessElement
    public void process(@Element TableRow element, OutputReceiver<Integer> output) {
      if ((Boolean) element.get("tornado")) {
        output.output(Integer.parseInt((String) element.get("month")));
      }
    }
  }

  @VisibleForTesting
  static class FormatCountsFn extends DoFn<KV<Integer, Long>, TableRow> {
    @ProcessElement
    public void process(@Element KV<Integer, Long> element, OutputReceiver<TableRow> output) {
      final TableRow row =
          new TableRow().set("month", element.getKey()).set("tornado_count", element.getValue());

      output.output(row);
    }
  }
}
