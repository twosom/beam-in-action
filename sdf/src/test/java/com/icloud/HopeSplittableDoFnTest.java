package com.icloud;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.transforms.DoFn.BoundedPerElement;
import static org.apache.beam.sdk.transforms.DoFn.UnboundedPerElement;
import static org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.TruncateResult;
import static org.apache.beam.sdk.values.PCollection.IsBounded;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Reify;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimator;
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators;
import org.apache.beam.sdk.transforms.windowing.GlobalWindow;
import org.apache.beam.sdk.transforms.windowing.IntervalWindow;
import org.apache.beam.sdk.transforms.windowing.Never;
import org.apache.beam.sdk.transforms.windowing.SlidingWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(JUnit4.class)
public class HopeSplittableDoFnTest {

  private static final Logger LOG = LoggerFactory.getLogger(HopeSplittableDoFnTest.class);

  static class PairStringWithIndexToLengthBase extends DoFn<String, KV<String, Integer>> {

    private static final Logger LOG =
        LoggerFactory.getLogger(PairStringWithIndexToLengthBase.class);

    @GetInitialRestriction
    public HopeOffsetRange getInitialRestriction(@Element String element) {
      LOG.info("[GetInitialRestriction] create initial restriction with value {}", element);
      return HopeOffsetRange.of(0, element.length());
    }

    @GetRestrictionCoder
    public Coder<HopeOffsetRange> getRestrictionCoder() throws NoSuchSchemaException {
      return SchemaRegistry.createDefault().getSchemaCoder(HopeOffsetRange.class);
    }

    @SplitRestriction
    public void splitRestriction(
        @Element String element,
        @Restriction HopeOffsetRange restriction,
        OutputReceiver<HopeOffsetRange> output) {
      final long from = restriction.getFrom();
      final long to = restriction.getTo();
      final long half = (from + to) / 2;

      HopeOffsetRange split = HopeOffsetRange.of(from, half);
      output.output(split);
      LOG.info(
          "[@SplitRestriction] emit split restriction with value {}, current element {}",
          split,
          element);
      split = HopeOffsetRange.of(half, to);
      output.output(split);
      LOG.info(
          "[@SplitRestriction] emit split restriction with value {}, current element {}",
          split,
          element);
    }

    @ProcessElement
    public ProcessContinuation process(
        @Element String element,
        RestrictionTracker<HopeOffsetRange, Long> tracker,
        OutputReceiver<KV<String, Integer>> output,
        @Timestamp Instant timestamp) {
      final HopeOffsetRange restriction =
          checkNotNull(tracker.currentRestriction(), "current restriction can not be null");

      for (long i = restriction.getFrom(), numIterations = 0;
          tracker.tryClaim(i);
          i++, numIterations++) {
        output.output(KV.of(element, (int) i));
        LOG.info("[@ProcessElement] emit {}, timestamp = {}", element, timestamp);
        if (numIterations % 3 != 0) {
          LOG.info(
              "[@ProcessElement] numIteration is not part of 3, current value = {}", numIterations);
          return ProcessContinuation.resume();
        }
      }
      return ProcessContinuation.stop();
    }
  }

  @BoundedPerElement
  static class PairStringWithIndexToLengthBounded extends PairStringWithIndexToLengthBase {}

  @UnboundedPerElement
  static class PairStringWithIndexToLengthUnbounded extends PairStringWithIndexToLengthBase {}

  private static PairStringWithIndexToLengthBase pairStringWithIndexToLengthFn(IsBounded bounded) {
    return bounded == IsBounded.BOUNDED
        ? new PairStringWithIndexToLengthBounded()
        : new PairStringWithIndexToLengthUnbounded();
  }

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testPairWithIndexBasicBounded() {
    testPairWithIndexBasic(IsBounded.BOUNDED);
  }

  @Test
  public void testPairWithIndexBasicUnbounded() {
    testPairWithIndexBasic(IsBounded.UNBOUNDED);
  }

  private void testPairWithIndexBasic(IsBounded bounded) {
    final PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a", "bb", "ccccc"))
            .apply(ParDo.of(pairStringWithIndexToLengthFn(bounded)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()))
            .apply(LogUtils.of());

    PAssert.that(res)
        .containsInAnyOrder(
            Arrays.asList(
                KV.of("a", 0),
                KV.of("bb", 0),
                KV.of("bb", 1),
                KV.of("ccccc", 0),
                KV.of("ccccc", 1),
                KV.of("ccccc", 2),
                KV.of("ccccc", 3),
                KV.of("ccccc", 4)));

    p.run();
  }

  @Test
  public void testPairWithIndexWindowedTimestampedBounded() {
    testPairWithIndexWindowedTimestamped(IsBounded.BOUNDED);
  }

  @Test
  public void testPairWithIndexWindowedTimestampedUnbounded() {
    testPairWithIndexWindowedTimestamped(IsBounded.UNBOUNDED);
  }

  /**
   * Tests that Splittable DoFn correctly propagates windowing strategy, windows and timestamps of
   * elements in the input collection.
   */
  private void testPairWithIndexWindowedTimestamped(IsBounded bounded) {
    final MutableDateTime mutableNow = Instant.now().toMutableDateTime();
    mutableNow.setMillisOfSecond(0);
    final Instant now = mutableNow.toInstant();
    final Instant nowP1 = now.plus(Duration.standardSeconds(1));
    final Instant nowP2 = now.plus(Duration.standardSeconds(2));

    final long SLIDING_WINDOW_SIZE = 5L;

    final SlidingWindows windowFn =
        SlidingWindows.of(Duration.standardSeconds(SLIDING_WINDOW_SIZE))
            .every(Duration.standardSeconds(1L));

    final PCollection<KV<String, Integer>> res =
        p.apply(
                Create.timestamped(
                    TimestampedValue.of("a", now),
                    TimestampedValue.of("bb", nowP1),
                    TimestampedValue.of("ccccc", nowP2)))
            .apply(Window.into(windowFn))
            .apply(ParDo.of(pairStringWithIndexToLengthFn(bounded)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    assertEquals(windowFn, res.getWindowingStrategy().getWindowFn());

    final PCollection<TimestampedValue<KV<String, Integer>>> timestamped =
        res.apply(Reify.timestamps());

    for (int i = 0; i < 4; i++) {
      final Instant base = now.minus(Duration.standardSeconds(i));
      LOG.info("base = {}", base);
      final IntervalWindow window =
          new IntervalWindow(base, base.plus(Duration.standardSeconds(SLIDING_WINDOW_SIZE)));

      final List<TimestampedValue<KV<String, Integer>>> expectedUnfiltered =
          Arrays.asList(
              TimestampedValue.of(KV.of("a", 0), now),
              TimestampedValue.of(KV.of("bb", 0), nowP1),
              TimestampedValue.of(KV.of("bb", 1), nowP1),
              TimestampedValue.of(KV.of("ccccc", 0), nowP2),
              TimestampedValue.of(KV.of("ccccc", 1), nowP2),
              TimestampedValue.of(KV.of("ccccc", 2), nowP2),
              TimestampedValue.of(KV.of("ccccc", 3), nowP2),
              TimestampedValue.of(KV.of("ccccc", 4), nowP2));

      final List<TimestampedValue<KV<String, Integer>>> expected = new ArrayList<>();

      for (TimestampedValue<KV<String, Integer>> tv : expectedUnfiltered) {

        /*
          window start -------> tv timestamp -------> window end
        */
        if (!window.start().isAfter(tv.getTimestamp())
            && !tv.getTimestamp().isAfter(window.maxTimestamp())) {
          expected.add(tv);
        }
      }

      assertFalse(expected.isEmpty());

      PAssert.that(timestamped).inWindow(window).containsInAnyOrder(expected);
    }
    p.run();
  }

  private static class SDFWithMultipleOutputsPerBlockBase extends DoFn<String, Integer> {

    private static final int MAX_INDEX = 98765;

    private static final Logger LOG =
        LoggerFactory.getLogger(SDFWithMultipleOutputsPerBlockBase.class);

    private final int numClaimsPerCall;

    private SDFWithMultipleOutputsPerBlockBase(final int numClaimsPerCall) {
      this.numClaimsPerCall = numClaimsPerCall;
    }

    private static int snapToNextBlock(final int target, final int[] blockStarts) {
      for (int index = 1; index < blockStarts.length; index++) {
        if (target > blockStarts[index - 1] && target <= blockStarts[index]) {
          return index;
        }
      }
      throw new IllegalStateException("Shouldn't get here");
    }

    @GetInitialRestriction
    public HopeOffsetRange getInitialRestriction() {
      return HopeOffsetRange.of(0, MAX_INDEX);
    }

    @GetRestrictionCoder
    public Coder<HopeOffsetRange> getRestrictionCoder() throws NoSuchSchemaException {
      return SchemaRegistry.createDefault().getSchemaCoder(HopeOffsetRange.class);
    }

    @ProcessElement
    public ProcessContinuation process(
        @Element String element,
        OutputReceiver<Integer> output,
        RestrictionTracker<HopeOffsetRange, Long> tracker) {
      final int[] blockStarts = {-1, 0, 12, 123, 1234, 12345, 34567, MAX_INDEX};
      final HopeOffsetRange restriction =
          checkNotNull(tracker.currentRestriction(), "current restriction can not be null");

      final int trueStarts = snapToNextBlock((int) restriction.getFrom(), blockStarts);
      for (int i = trueStarts, numIterations = 1;
          tracker.tryClaim((long) blockStarts[i]);
          i++, numIterations++) {
        for (int index = blockStarts[i]; index < blockStarts[i + 1]; index++) {
          output.output(index);
        }

        if (numIterations == numClaimsPerCall) {
          LOG.info("[@ProcessElement] num iterations equals with num claims per call");
          return ProcessContinuation.resume();
        }
      }
      return ProcessContinuation.stop();
    }
  }

  @BoundedPerElement
  private static class SDFWithMultipleOutputsPerBlockBounded
      extends SDFWithMultipleOutputsPerBlockBase {
    SDFWithMultipleOutputsPerBlockBounded(int numClaimsPerCall) {
      super(numClaimsPerCall);
    }
  }

  @UnboundedPerElement
  private static class SDFWithMultipleOutputsPerBlockUnbounded
      extends SDFWithMultipleOutputsPerBlockBase {
    SDFWithMultipleOutputsPerBlockUnbounded(int numClaimsPerCall) {
      super(numClaimsPerCall);
    }
  }

  private static SDFWithMultipleOutputsPerBlockBase sdfWithMultipleOutputsPerBlock(
      IsBounded bounded, int numClaimsPerCall) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithMultipleOutputsPerBlockBounded(numClaimsPerCall)
        : new SDFWithMultipleOutputsPerBlockUnbounded(numClaimsPerCall);
  }

  @Test
  public void testOutputAfterCheckpointBounded() {
    testOutputAfterCheckpoint(IsBounded.BOUNDED);
  }

  @Test
  public void testOutputAfterCheckpointUnbounded() {
    testOutputAfterCheckpoint(IsBounded.UNBOUNDED);
  }

  private void testOutputAfterCheckpoint(IsBounded bounded) {
    final PCollection<Integer> outputs =
        p.apply(Create.of("foo"))
            .apply(ParDo.of(sdfWithMultipleOutputsPerBlock(bounded, 3)))
            .apply(Window.<Integer>configure().triggering(Never.ever()).discardingFiredPanes());

    PAssert.thatSingleton(outputs.apply(Count.globally()))
        .isEqualTo((long) SDFWithMultipleOutputsPerBlockBase.MAX_INDEX);
    p.run();
  }

  private static class SDFWithSideInputBase extends DoFn<Integer, String> {
    private final Map<Instant, String> expectedSideInputValues;

    SDFWithSideInputBase(Map<Instant, String> expectedSideInputValues) {
      this.expectedSideInputValues = expectedSideInputValues;
    }

    @GetInitialRestriction
    public HopeOffsetRange getInitialRestriction(
        @SideInput("sideInput") String sideInput, @Timestamp Instant timestamp) {
      assertThat(this.expectedSideInputValues, hasEntry(timestamp, sideInput));
      return HopeOffsetRange.of(0, 1);
    }

    @GetRestrictionCoder
    public Coder<HopeOffsetRange> getRestrictionCoder() throws NoSuchSchemaException {
      return SchemaRegistry.createDefault().getSchemaCoder(HopeOffsetRange.class);
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimate(
        @SideInput("sideInput") String sideInput, @Timestamp Instant timestamp) {
      assertThat(this.expectedSideInputValues, hasEntry(timestamp, sideInput));
      return timestamp;
    }

    @GetSize
    public double getSize(
        @Restriction HopeOffsetRange restriction,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(this.expectedSideInputValues, hasEntry(timestamp, sideInput));
      return restriction.getTo() - restriction.getFrom();
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction HopeOffsetRange restriction,
        OutputReceiver<HopeOffsetRange> output,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(this.expectedSideInputValues, hasEntry(timestamp, sideInput));
      output.output(restriction);
    }

    @TruncateRestriction
    public RestrictionTracker.TruncateResult<HopeOffsetRange> truncate(
        @Restriction HopeOffsetRange restriction,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(this.expectedSideInputValues, hasEntry(timestamp, sideInput));
      return TruncateResult.of(restriction);
    }

    @NewTracker
    public RestrictionTracker<HopeOffsetRange, Long> newTracker(
        @Restriction HopeOffsetRange restriction,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(this.expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new HopeOffsetRangeTracker(restriction);
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimateState,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(this.expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new WatermarkEstimators.MonotonicallyIncreasing(watermarkEstimateState);
    }

    @ProcessElement
    public void process(
        @Element Integer element,
        RestrictionTracker<HopeOffsetRange, Long> tracker,
        @SideInput("sideInput") String sideInput,
        OutputReceiver<String> output) {
      final HopeOffsetRange restriction =
          checkNotNull(tracker.currentRestriction(), "current restriction can not be null");

      checkState(tracker.tryClaim(restriction.getFrom()));
      output.output(sideInput + ":" + element);
    }
  }

  @Test
  public void testSideInputBounded() {
    testSideInput(IsBounded.BOUNDED);
  }

  @Test
  public void testSideInputUnbounded() {
    testSideInput(IsBounded.UNBOUNDED);
  }

  @BoundedPerElement
  private static class SDFWithSideInputBounded extends SDFWithSideInputBase {
    SDFWithSideInputBounded(Map<Instant, String> expectedSideInputValues) {
      super(expectedSideInputValues);
    }
  }

  @UnboundedPerElement
  private static class SDFWithSideInputUnbounded extends SDFWithSideInputBase {
    SDFWithSideInputUnbounded(Map<Instant, String> expectedSideInputValues) {
      super(expectedSideInputValues);
    }
  }

  private static SDFWithSideInputBase sdfWithSideInput(
      IsBounded bounded, Map<Instant, String> expectedSideInputValues) {
    return bounded == IsBounded.BOUNDED
        ? new SDFWithSideInputBounded(expectedSideInputValues)
        : new SDFWithSideInputUnbounded(expectedSideInputValues);
  }

  private void testSideInput(IsBounded bounded) {
    final PCollectionView<String> sideInput =
        p.apply("Side Input", Create.of("foo")).apply(View.asSingleton());

    final Map<Instant, String> expectedSideInput =
        Collections.singletonMap(GlobalWindow.TIMESTAMP_MIN_VALUE, "foo");

    final PCollection<String> res =
        p.apply("input", Create.of(0, 1, 2))
            .apply(
                ParDo.of(sdfWithSideInput(bounded, expectedSideInput))
                    .withSideInput("sideInput", sideInput));
    PAssert.that(res).containsInAnyOrder("foo:0", "foo:1", "foo:2");

    p.run();
  }
}
