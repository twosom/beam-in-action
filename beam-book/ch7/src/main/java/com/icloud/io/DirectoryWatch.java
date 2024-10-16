package com.icloud.io;

import static com.google.common.base.Preconditions.*;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.InstantCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Impulse;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DirectoryWatch {

  public static Read read(String path) {
    return new AutoValue_DirectoryWatch_Read.Builder()
        .setPath(path)
        .setWatermarkFn(KV::getValue)
        .build();
  }

  @AutoValue
  public abstract static class Read extends PTransform<PBegin, PCollection<String>> {

    abstract String path();

    abstract SerializableFunction<KV<String, Instant>, Instant> watermarkFn();

    abstract Builder toBuilder();

    @AutoValue.Builder
    abstract static class Builder {

      abstract Builder setPath(String path);

      abstract Builder setWatermarkFn(
          SerializableFunction<KV<String, Instant>, Instant> watermarkFn);

      abstract Read build();
    }

    @Override
    public PCollection<String> expand(PBegin input) {
      checkArgument(path() != null, "path can not be null");
      checkArgument(watermarkFn() != null, "watermarkFn can not be null");

      return input
          .apply(Impulse.create())
          .apply(MapElements.into(TypeDescriptors.strings()).via(e -> path()))
          .apply(ParDo.of(new DirectoryWatchFn(watermarkFn())));
    }

    public Read withPath(String path) {
      return this.toBuilder().setPath(path).build();
    }

    public Read withWatermarkFn(SerializableFunction<KV<String, Instant>, Instant> watermarkFn) {
      return this.toBuilder().setWatermarkFn(watermarkFn).build();
    }
  }

  @DoFn.UnboundedPerElement
  private static class DirectoryWatchFn extends DoFn<String, String> {

    private static final Logger LOG = LoggerFactory.getLogger(DirectoryWatchFn.class);
    private final SerializableFunction<KV<String, Instant>, Instant> watermarkFn;

    public DirectoryWatchFn(SerializableFunction<KV<String, Instant>, Instant> watermarkFn) {
      this.watermarkFn = watermarkFn;
    }

    @GetInitialRestriction
    public DirectoryWatchRestriction getInitialRestriction() {
      LOG.info("[@GetInitialRestriction] called at {}", Instant.now());
      return new DirectoryWatchRestriction(new HashSet<>());
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction DirectoryWatchRestriction restriction,
        OutputReceiver<DirectoryWatchRestriction> outputReceiver) {
      LOG.info("[@SplitRestriction] triggered at {}", Instant.now());
      outputReceiver.output(restriction);
    }

    @GetRestrictionCoder
    public Coder<DirectoryWatchRestriction> getRestrictionCoder() {

      LOG.info("[@GetRestrictionCoder] called at {}", Instant.now());
      return DirectoryWatchRestrictionCoder.of();
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimateState() {
      LOG.info("[@GetInitialWatermarkEstimatorState] called at {}", Instant.now());
      return BoundedWindow.TIMESTAMP_MIN_VALUE;
    }

    @GetWatermarkEstimatorStateCoder
    public Coder<Instant> getWatermarkEstimatorStateCoder() {
      LOG.info("[@GetWatermarkEstimatorStateCoder] called at {}", Instant.now());
      return InstantCoder.of();
    }

    @NewWatermarkEstimator
    public WatermarkEstimators.Manual newWatermarkEstimator(
        @WatermarkEstimatorState Instant initialWatermark) {
      LOG.info("[@NewWatermarkEstimator] called at {}", Instant.now());
      return new WatermarkEstimators.Manual(initialWatermark);
    }

    @ProcessElement
    public ProcessContinuation processElement(
        @Element String path,
        RestrictionTracker<DirectoryWatchRestriction, String> tracker,
        ManualWatermarkEstimator<Instant> watermarkEstimator,
        OutputReceiver<String> output) {
      checkArgument(path != null, "path can not be null");
      while (true) {
        final List<KV<String, Instant>> newFiles = getNewFilesIfAny(path, tracker);
        if (newFiles.isEmpty()) {
          LOG.info("[@ProcessElement] there is no more new files");
          return ProcessContinuation.resume().withResumeDelay(Duration.millis(500));
        }

        Instant maxInstant = watermarkEstimator.currentWatermark();

        for (KV<String, Instant> newFile : newFiles) {
          if (!tracker.tryClaim(newFile.getKey())) {
            watermarkEstimator.setWatermark(maxInstant);
            return ProcessContinuation.stop();
          }

          output.outputWithTimestamp(newFile.getKey(), newFile.getValue());
          final Instant fileWatermark = this.watermarkFn.apply(newFile);
          if (maxInstant.isBefore(fileWatermark)) {
            maxInstant = fileWatermark;
          }
        }

        watermarkEstimator.setWatermark(maxInstant);
        if (!maxInstant.isBefore(BoundedWindow.TIMESTAMP_MAX_VALUE)) {
          return ProcessContinuation.stop();
        }
      }
    }

    private List<KV<String, Instant>> getNewFilesIfAny(
        String path, RestrictionTracker<DirectoryWatchRestriction, String> tracker) {
      try (Stream<Path> stream = Files.walk(Paths.get(path), FileVisitOption.FOLLOW_LINKS)) {
        return stream
            .filter(file -> Files.isReadable(file) && !Files.isDirectory(file))
            .filter(
                file ->
                    !tracker.currentRestriction().getAlreadyProcessed().contains(file.toString()))
            .map(
                file -> {
                  try {
                    final BasicFileAttributes attr =
                        Files.readAttributes(file, BasicFileAttributes.class);
                    return KV.of(
                        file.toString(), Instant.ofEpochMilli(attr.creationTime().toMillis()));
                  } catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                })
            .collect(Collectors.toList());

      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
