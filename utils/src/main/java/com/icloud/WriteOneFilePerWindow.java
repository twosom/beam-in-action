package com.icloud;

import static com.google.common.base.MoreObjects.firstNonNull;
import static org.apache.beam.sdk.io.FileBasedSink.*;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.ResolveOptions;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

public class WriteOneFilePerWindow
    extends PTransform<@NonNull PCollection<String>, @NonNull PDone> {

  private static final DateTimeFormatter FORMATTER = ISODateTimeFormat.hourMinute();

  private String filenamePrefix;

  @Nullable private Integer numShards;

  public WriteOneFilePerWindow(String filenamePrefix, Integer numShards) {
    this.filenamePrefix = filenamePrefix;
    this.numShards = numShards;
  }

  @Override
  public PDone expand(PCollection<String> input) {
    final ResourceId resource = convertToFileResourceIfPossible(filenamePrefix);
    TextIO.Write write =
        TextIO.write()
            .to(new PerWindowFiles(resource))
            .withTempDirectory(resource.getCurrentDirectory())
            .withWindowedWrites();
    if (this.numShards != null) {
      write = write.withNumShards(this.numShards);
    }

    return input.apply(write);
  }

  public static class PerWindowFiles extends FilenamePolicy {

    private final ResourceId baseFilename;

    public PerWindowFiles(ResourceId baseFilename) {
      this.baseFilename = baseFilename;
    }

    public String filenamePrefixForWindow(IntervalWindow window) {
      final String prefix =
          this.baseFilename.isDirectory() ? "" : firstNonNull(this.baseFilename.getFilename(), "");
      return String.format(
          "%s-%s-%s", prefix, FORMATTER.print(window.start()), FORMATTER.print(window.end()));
    }

    @Override
    public ResourceId windowedFilename(
        int shardNumber,
        int numShards,
        BoundedWindow window,
        PaneInfo paneInfo,
        OutputFileHints outputFileHints) {
      final IntervalWindow intervalWindow = (IntervalWindow) window;
      final String filename =
          String.format(
              "%s-%s-of-%s%s",
              this.filenamePrefixForWindow(intervalWindow),
              shardNumber,
              numShards,
              outputFileHints.getSuggestedFilenameSuffix());

      return this.baseFilename
          .getCurrentDirectory()
          .resolve(filename, ResolveOptions.StandardResolveOptions.RESOLVE_FILE);
    }

    @Override
    public ResourceId unwindowedFilename(
        int shardNumber, int numShards, OutputFileHints outputFileHints) {
      throw new UnsupportedOperationException("Unsupported.");
    }
  }
}
