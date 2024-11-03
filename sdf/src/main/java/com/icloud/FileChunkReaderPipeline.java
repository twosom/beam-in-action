package com.icloud;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.MetricNameFilter;
import org.apache.beam.sdk.metrics.MetricQueryResults;
import org.apache.beam.sdk.metrics.MetricResult;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.metrics.MetricsFilter;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileChunkReaderPipeline {

  /** Read File Chunk function */
  @DoFn.BoundedPerElement
  private static final class ReadFileChunkFn extends DoFn<String, String> {

    private final Counter readBytesCounter =
        Metrics.counter(ReadFileChunkFn.class, "read_bytes_counter");

    /** the chunk size (1MB) */
    private static final int CHUNK_SIZE = 1 << 20;

    private static final Logger LOG = LoggerFactory.getLogger(ReadFileChunkFn.class);
    public static final int END_OF_FILE = -1;

    @GetInitialRestriction
    public HopeOffsetRange getInitialRestriction(@Element String filePath) {
      try {
        final long size = Files.size(Path.of(filePath));
        LOG.info("[@GetInitialRestriction] get file {} size {}", filePath, size);
        return HopeOffsetRange.of(0, size);
      } catch (IOException e) {
        LOG.error("[@GetInitialRestriction] unable to get file size {} with error: ", filePath, e);
        throw new RuntimeException(e);
      }
    }

    @GetRestrictionCoder
    public Coder<HopeOffsetRange> getRestrictionCoder() throws NoSuchSchemaException {
      return SchemaRegistry.createDefault().getSchemaCoder(HopeOffsetRange.class);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction HopeOffsetRange restriction, OutputReceiver<HopeOffsetRange> output) {
      LOG.info("[@SplitRestriction] called at {} with restriction {}", Instant.now(), restriction);
      long start = restriction.getFrom();
      final long end = restriction.getTo();
      final long size = end - start;
      if (size <= CHUNK_SIZE) {
        LOG.info(
            "[@SplitRestriction] tried to split restriction {} but current file size is lower than chunk size {}",
            restriction,
            CHUNK_SIZE);
        output.output(restriction);
        return;
      }

      while (start < end - CHUNK_SIZE) {
        final HopeOffsetRange splitRestriction = HopeOffsetRange.of(start, start + CHUNK_SIZE);
        LOG.info("[@SplitRestriction] split restriction with {}", splitRestriction);
        output.output(splitRestriction);
        start += CHUNK_SIZE;
      }

      final HopeOffsetRange splitRestriction = HopeOffsetRange.of(start, end);
      LOG.info("[@SplitRestriction] split restriction with {}", splitRestriction);
      output.output(splitRestriction);
    }

    @ProcessElement
    public void process(
        @Element String filePath,
        RestrictionTracker<HopeOffsetRange, Long> tracker,
        OutputReceiver<String> output) {
      LOG.info("[@ProcessElement] called at {}", Instant.now());
      final HopeOffsetRange restriction = tracker.currentRestriction();
      final long start = restriction.getFrom();
      final long end = restriction.getTo();
      final long size = end - start;
      final int bytesToRead = (int) Math.min(size, CHUNK_SIZE);

      try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
        file.seek(start);
        while (tracker.tryClaim(file.getFilePointer())) {
          final byte[] buffer = new byte[bytesToRead];
          final int readBytes = file.read(buffer, 0, bytesToRead);
          if (readBytes != END_OF_FILE) {
            final String value = new String(buffer, 0, readBytes);
            output.output(value);
            this.readBytesCounter.inc(readBytes);
          }
        }
      } catch (FileNotFoundException e) {
        LOG.error("[@ProcessElement] unable to find file {}", filePath, e);
        throw new RuntimeException(e);
      } catch (IOException e) {
        LOG.error("[@ProcessElement] unable process with file {}", filePath, e);
        throw new RuntimeException(e);
      }
    }
  }

  public static void main(String[] args) {
    final Pipeline p = PipelineUtils.create(args);
    p.apply("Emit Path", Create.of("/Users/hope/Documents/users.xml"))
        .apply("Read File Chunk", ParDo.of(new ReadFileChunkFn()))
        .apply(LogUtils.of());

    final PipelineResult pipelineResult = p.run();
    final MetricQueryResults metricQueryResult =
        pipelineResult
            .metrics()
            .queryMetrics(
                MetricsFilter.builder()
                    .addNameFilter(MetricNameFilter.inNamespace(ReadFileChunkFn.class))
                    .build());

    for (final MetricResult<Long> counter : metricQueryResult.getCounters()) {
      System.out.println(counter.getAttempted());
      System.out.println(counter.getCommitted());
    }
  }
}
