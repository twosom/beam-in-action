package com.icloud.io;

import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileRead {

  public static Read read() {
    return new AutoValue_FileRead_Read();
  }

  @AutoValue
  public abstract static class Read extends PTransform<PCollection<String>, PCollection<String>> {
    @Override
    public PCollection<String> expand(PCollection<String> input) {
      return input.apply(ParDo.of(new FileReadFn()));
    }
  }

  @DoFn.BoundedPerElement
  private static class FileReadFn extends DoFn<String, String> {
    private static final Logger LOG = LoggerFactory.getLogger(FileReadFn.class);

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element String path) throws IOException {
      return new OffsetRange(0, Files.size(Paths.get(path)));
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange restriction, OutputReceiver<OffsetRange> outputReceiver) {
      LOG.info(
          "[@SplitRestriction] triggered at {}, restriction size = {}",
          Instant.now(),
          restriction.getTo() - restriction.getFrom());
      long splitSize = 32 * (1 << 20);
      long i = restriction.getFrom();
      while (i < restriction.getTo() - splitSize) {
        // Compute and output 64 MiB size ranges to process in parallel
        long end = i + splitSize;
        LOG.info(
            "[@SplitRestriction] {} -> {}", restriction.getTo() - restriction.getFrom(), end - i);
        outputReceiver.output(new OffsetRange(i, end));
        i = end;
      }
      // Output the last range
      outputReceiver.output(new OffsetRange(i, restriction.getTo()));
    }

    @GetRestrictionCoder
    public Coder<OffsetRange> getRestrictionCoder() {
      return OffsetRange.Coder.of();
    }

    @ProcessElement
    public void process(
        @Element String path,
        RestrictionTracker<OffsetRange, Long> tracker,
        OutputReceiver<String> output)
        throws IOException {
      final long position = tracker.currentRestriction().getFrom();

      try (RandomAccessFile file = new RandomAccessFile(path, "r")) {
        seekFileToStartLine(position, file);
        while (tracker.tryClaim(file.getFilePointer())) {
          output.output(file.readLine());
        }
      }
    }

    private void seekFileToStartLine(long position, RandomAccessFile file) throws IOException {

      final Byte precedingByte;
      if (position == 0) {
        precedingByte = null;
      } else {
        file.seek(position - 1);
        precedingByte = file.readByte();
      }
      if (precedingByte == null || '\n' == precedingByte) {
        return;
      }

      file.readLine();
    }
  }
}
