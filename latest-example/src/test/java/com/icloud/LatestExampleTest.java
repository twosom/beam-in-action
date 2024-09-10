package com.icloud;

import static com.google.common.collect.Lists.reverse;
import static java.util.stream.Collectors.toList;
import static org.joda.time.Instant.now;

import java.util.List;
import java.util.stream.IntStream;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Latest;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.values.PCollection;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class LatestExampleTest {

  private Pipeline pipeline;

  private List<Integer> elements;

  @BeforeEach
  void beforeEach() {
    this.pipeline = PipelineUtils.create();
    this.elements = reverse(IntStream.rangeClosed(1, 10).boxed().collect(toList()));
  }

  @Test
  void latestWithTimestampedMustHaveMaxTimestampedValue() {
    // given
    final Instant baseInstant = baseInstantFrom(now());

    final PCollection<Integer> withTimestamps =
        pipeline
            .apply(Create.of(this.elements))
            .apply(WithTimestamps.of(i -> baseInstant.plus(Duration.standardSeconds(i))));

    // when
    final PCollection<Integer> latestTimestamped = withTimestamps.apply(Latest.globally());

    // then
    // max timestamped value is 10
    PAssert.that(latestTimestamped).containsInAnyOrder(10);

    pipeline.run();
  }

  private Instant baseInstantFrom(Instant now) {
    return now.minus(Duration.standardMinutes(1));
  }
}
