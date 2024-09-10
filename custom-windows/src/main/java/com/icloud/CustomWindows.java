package com.icloud;

import static org.apache.beam.sdk.values.TypeDescriptors.*;

import java.util.Collection;
import java.util.Collections;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Sample;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomWindows {

  private static final Logger LOG = LoggerFactory.getLogger(CustomWindows.class);

  public static void main(String[] args) {
    final Pipeline pipeline = PipelineUtils.create(args, CustomWindowsOptions.class);
    final CustomWindowsOptions options = pipeline.getOptions().as(CustomWindowsOptions.class);

    pipeline
        .apply("ReadFromPubSub", PubsubIO.readStrings().fromTopic(options.getTopic()))
        .apply(
            "to KV",
            MapElements.into(kvs(strings(), integers()))
                .via(
                    (String message) -> {
                      final JSONObject json = new JSONObject(message);
                      final String rideId = json.getString("ride_id");
                      final int passengerCount = json.getInt("passenger_count");
                      return KV.of(rideId, passengerCount);
                    }))
        .apply(LogUtils.of())
        .apply(Window.into(new WindowByValueElement()))
        .apply(Sample.fixedSizePerKey(1))
        .apply(
            "Log window size",
            ParDo.of(
                new DoFn<KV<String, Iterable<Integer>>, String>() {
                  @ProcessElement
                  public void process(
                      @Element KV<String, Iterable<Integer>> element,
                      BoundedWindow window,
                      OutputReceiver<String> output) {
                    LOG.info("element {} in window {}", element, window);
                    output.output(element.toString());
                  }
                }))
        .apply(LogUtils.of());
    pipeline.run().waitUntilFinish();
  }

  public interface CustomWindowsOptions extends PipelineOptions {

    @Description("Topic to read from")
    @Default.String("projects/pubsub-public-data/topics/taxirides-realtime")
    String getTopic();

    void setTopic(String value);
  }

  static class WindowByValueElement extends WindowFn<KV<String, Integer>, @NonNull IntervalWindow> {

    @Override
    public Collection<IntervalWindow> assignWindows(AssignContext c) {
      final Instant timestamp = c.timestamp();
      final int passengers = c.element().getValue();
      final Duration size = Duration.standardMinutes(passengers * 10L);
      final Instant start =
          new Instant(timestamp.getMillis() - timestamp.plus(size).getMillis() % size.getMillis());
      final Instant end = start.plus(size);
      return Collections.singletonList(new IntervalWindow(start, end));
    }

    @Override
    public void mergeWindows(MergeContext c) {}

    @Override
    public boolean isCompatible(WindowFn<?, ?> other) {
      return this.equals(other);
    }

    @Override
    public Coder<IntervalWindow> windowCoder() {
      return IntervalWindow.getCoder();
    }

    @Override
    public WindowMappingFn<IntervalWindow> getDefaultWindowMappingFn() {
      return null;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof WindowByValueElement;
    }

    @Override
    public void verifyCompatibility(WindowFn<?, ?> other) throws IncompatibleWindowException {
      if (!this.isCompatible(other)) {
        throw new IncompatibleWindowException(
            other,
            String.format(
                "Only %s objects with the same sizes and key to check are compatible.",
                WindowByValueElement.class.getSimpleName()));
      }
    }
  }
}
