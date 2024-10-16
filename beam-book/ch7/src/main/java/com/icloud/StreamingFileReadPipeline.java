package com.icloud;

import com.icloud.io.DirectoryWatch;
import com.icloud.io.FileRead;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Reshuffle;

public class StreamingFileReadPipeline {

  public static void main(String[] args) {
    final Pipeline p = PipelineUtils.create(args);
    p.apply(DirectoryWatch.read("/Users/hope/Downloads"))
        .apply(Reshuffle.viaRandomKey())
        .apply(FileRead.read())
        .apply(LogUtils.of())
    ;

    p.run();
  }
}
