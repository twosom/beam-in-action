package com.icloud.sdf;

import static com.google.common.base.Preconditions.*;
import static org.apache.beam.sdk.values.TypeDescriptors.*;
import static org.apache.beam.sdk.values.TypeDescriptors.doubles;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PiSampler {

  public interface RandomFactory extends Serializable {
    Random getRandom();
  }

  public static Process process() {
    return new AutoValue_PiSampler_Process.Builder().build();
  }

  @AutoValue
  public abstract static class Process extends PTransform<PBegin, PCollection<Double>> {

    @Nullable
    abstract Long numSamples();

    @Nullable
    abstract Long parallelism();

    @Nullable
    abstract RandomFactory randomFactory();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {
      abstract Builder setNumSamples(long numSamples);

      abstract Builder setParallelism(long parallelism);

      abstract Builder setRandomFactory(RandomFactory randomFactory);

      abstract Process build();
    }

    public Process withNumSamples(int numSamples) {
      return toBuilder().setNumSamples(numSamples).build();
    }

    public Process withParallelism(int parallelism) {
      return toBuilder().setParallelism(parallelism).build();
    }

    public Process withRandomFactory(RandomFactory randomFactory) {
      return toBuilder().setRandomFactory(randomFactory).build();
    }

    @Override
    public PCollection<Double> expand(PBegin input) {
      checkArgument(parallelism() != null, "parallelism() can not be null");
      checkArgument(numSamples() != null, "numSamples() can not be null");
      checkArgument(randomFactory() != null, "randomFactory() can not be null");

      return input
          .apply(Impulse.create())
          .apply(
              FlatMapElements.into(strings())
                  .via(
                      e ->
                          LongStream.range(0, parallelism())
                              .mapToObj(i -> "")
                              .collect(Collectors.toList())))
          .apply(Reshuffle.viaRandomKey())
          .apply(ParDo.of(new SampleDoFn(randomFactory(), numSamples())))
          .apply(Sum.longsGlobally())
          .apply(
              MapElements.into(doubles())
                  .via(c -> 4 * (1.0 - c / (double) (numSamples() * parallelism()))));
    }
  }

  private static class SampleDoFn extends DoFn<String, Long> {
    private static final Logger LOG = LoggerFactory.getLogger(SampleDoFn.class);

    private final RandomFactory randomFactory;
    private final Long numSamples;

    private SampleDoFn(RandomFactory randomFactory, Long numSamples) {
      this.randomFactory = randomFactory;
      this.numSamples = numSamples;
    }

    @GetInitialRestriction
    public OffsetRange initialRestriction() {
      LOG.info("[@GetInitialRestriction] triggered at {}", Instant.now());
      return new OffsetRange(0, numSamples);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> output) {
      LOG.info("[@SplitRestriction] triggered at {}", Instant.now());

      long splitSize = 5000;
      long i = restriction.getFrom();
      while (i < restriction.getTo() - splitSize) {
        final long end = i + splitSize;
        LOG.info(
            "[@SplitRestriction] {} -> {}", restriction.getTo() - restriction.getFrom(), end - i);
        output.output(new OffsetRange(i, end));
        i = end;
      }

      output.output(new OffsetRange(i, restriction.getTo()));
    }

    @GetRestrictionCoder
    public OffsetRange.Coder getRestrictionCoder() {
      return OffsetRange.Coder.of();
    }

    @ProcessElement
    public void process(
        RestrictionTracker<OffsetRange, Long> tracker, OutputReceiver<Long> output) {
      final Random random = this.randomFactory.getRandom();
      long offset = tracker.currentRestriction().getFrom();
      while (tracker.tryClaim(offset++)) {
        final double x = random.nextDouble();
        final double y = random.nextDouble();
        if (x * x + y * y > 1) {
          output.output(1L);
        }
      }
    }
  }
}
