package com.icloud;

import com.icloud.sdf.PiSampler;
import java.util.Random;
import org.apache.beam.sdk.Pipeline;

public class PiSamplerPipeline {
  public static void main(String[] args) {
    final Pipeline p = PipelineUtils.create(args);
    p.apply(
            PiSampler.process()
                .withParallelism(1000)
                .withNumSamples(1_000_000_000)
                .withRandomFactory(Random::new))
        .apply(LogUtils.of());

    p.run();
  }
}
