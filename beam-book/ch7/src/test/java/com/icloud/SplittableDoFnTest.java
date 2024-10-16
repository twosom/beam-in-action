package com.icloud;

import static com.google.common.base.Preconditions.checkState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import java.io.Serializable;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.testing.*;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.splittabledofn.*;
import org.apache.beam.sdk.transforms.windowing.*;
import org.apache.beam.sdk.values.*;
import org.apache.beam.sdk.values.PCollection.IsBounded;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.MutableDateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class SplittableDoFnTest implements Serializable {
  @Rule public transient Timeout globalTimeout = Timeout.seconds(1200);

  @SuppressWarnings("unused")
  static class PairStringWithIndexToLengthBase extends DoFn<String, KV<String, Integer>> {
    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      for (long i = tracker.currentRestriction().getFrom(), numIteration = 1;
          tracker.tryClaim(i);
          ++i, ++numIteration) {
        c.output(KV.of(c.element(), (int) i));
        if (numIteration % 3 == 0) {
          System.out.println(" [resume] current num iteration = " + numIteration);
          return ProcessContinuation.resume();
        }
      }
      System.out.println(" [stop] ");
      return ProcessContinuation.stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(@Element String element) {
      return new OffsetRange(0, element.length());
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      receiver.output(new OffsetRange(0, (range.getFrom() + range.getTo()) / 2));
      receiver.output(new OffsetRange((range.getFrom() + range.getTo()) / 2, range.getTo()));
    }
  }

  @DoFn.BoundedPerElement
  static class PairStringWithIndexToLengthBounded extends PairStringWithIndexToLengthBase {}

  @DoFn.UnboundedPerElement
  static class PairStringWithIndexToLengthUnbounded extends PairStringWithIndexToLengthBase {}

  private static PairStringWithIndexToLengthBase pairStringWithIndexToLengthFn(IsBounded bounded) {
    return (bounded == IsBounded.BOUNDED)
        ? new PairStringWithIndexToLengthBounded()
        : new PairStringWithIndexToLengthUnbounded();
  }

  @Rule public final transient TestPipeline p = TestPipeline.create();

  @Test
  public void testPairWithIndexBasicBounded() {
    testPairWithIndexBasic(IsBounded.BOUNDED);
  }

  @Test
  public void testPairWithIndexBaseUnbounded() {
    testPairWithIndexBasic(IsBounded.UNBOUNDED);
  }

  private void testPairWithIndexBasic(IsBounded bounded) {
    final PCollection<KV<String, Integer>> res =
        p.apply(Create.of("a", "bb", "ccccc"))
            .apply(ParDo.of(pairStringWithIndexToLengthFn(bounded)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

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

  private void testPairWithIndexWindowedTimestamped(IsBounded bounded) {
    final MutableDateTime mutableNow = Instant.now().toMutableDateTime();
    mutableNow.setMillisOfSecond(0);
    final Instant now = mutableNow.toInstant();

    final Instant nowP1 = now.plus(Duration.standardSeconds(1));
    final Instant nowP2 = now.plus(Duration.standardSeconds(2));

    final SlidingWindows windowFn =
        SlidingWindows.of(Duration.standardSeconds(5)).every(Duration.standardSeconds(1));

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

    for (int i = 0; i < 4; ++i) {
      final Instant base = now.minus(Duration.standardSeconds(i));
      final IntervalWindow window =
          new IntervalWindow(base, base.plus(Duration.standardSeconds(5)));

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

        if (tv.getTimestamp().isBefore(window.start())
            || tv.getTimestamp().isBefore(window.maxTimestamp())) {
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

    private final int numClaimsPerCall;

    SDFWithMultipleOutputsPerBlockBase(int numClaimsPerCall) {
      this.numClaimsPerCall = numClaimsPerCall;
    }

    private static int snapToNextBlock(int index, int[] blockStarts) {
      for (int i = 1; i < blockStarts.length; i++) {
        if (index > blockStarts[i - 1] && index <= blockStarts[i]) {
          return i;
        }
      }
      throw new IllegalStateException("Shouldn't get here");
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      int[] blockStarts = {-1, 0, 12, 123, 1234, 12345, 34567, MAX_INDEX};
      final int trueStarts =
          snapToNextBlock((int) tracker.currentRestriction().getFrom(), blockStarts);

      for (int i = trueStarts, numIteration = 1;
          tracker.tryClaim((long) blockStarts[i]);
          ++i, ++numIteration) {
        for (int index = blockStarts[i]; index < blockStarts[i + 1]; ++index) {
          c.output(index);
        }
        if (numIteration == numClaimsPerCall) {
          return ProcessContinuation.resume();
        }
      }
      return ProcessContinuation.stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRange() {
      return new OffsetRange(0, MAX_INDEX);
    }
  }

  @DoFn.BoundedPerElement
  private static class SDFWithMultipleOutputsPerBlockBounded
      extends SDFWithMultipleOutputsPerBlockBase {
    private SDFWithMultipleOutputsPerBlockBounded(int numClaimsPerCell) {
      super(numClaimsPerCell);
    }
  }

  @DoFn.UnboundedPerElement
  private static class SDFWithMultipleOutputsPerBlockUnbounded
      extends SDFWithMultipleOutputsPerBlockBase {
    private SDFWithMultipleOutputsPerBlockUnbounded(int numClaimsPerCell) {
      super(numClaimsPerCell);
    }
  }

  private static SDFWithMultipleOutputsPerBlockBase sdfWithMultipleOutputsPerBlock(
      IsBounded bounded, int numClaimsPerCell) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithMultipleOutputsPerBlockBounded(numClaimsPerCell)
        : new SDFWithMultipleOutputsPerBlockUnbounded(numClaimsPerCell);
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

    @ProcessElement
    public void process(
        ProcessContext c,
        RestrictionTracker<OffsetRange, Long> tracker,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      checkState(tracker.tryClaim(tracker.currentRestriction().getFrom()));
      c.output(sideInput + ":" + c.element());
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(
        @SideInput("sideInput") String sideInput, @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new OffsetRange(0, 1);
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(
        @SideInput("sideInput") String sideInput, @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return timestamp;
    }

    @GetSize
    public double getSize(
        @Restriction OffsetRange range,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return range.getTo() - range.getFrom();
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange restriction,
        OutputReceiver<OffsetRange> splitReceiver,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      splitReceiver.output(restriction);
    }

    @TruncateRestriction
    public RestrictionTracker.TruncateResult<OffsetRange> truncate(
        @Restriction OffsetRange restriction,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return RestrictionTracker.TruncateResult.of(restriction);
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(
        @Restriction OffsetRange restriction,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new OffsetRangeTracker(restriction);
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimateState,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new WatermarkEstimators.MonotonicallyIncreasing(watermarkEstimateState);
    }
  }

  @DoFn.BoundedPerElement
  private static class SDFWithSideInputBounded extends SDFWithSideInputBase {
    SDFWithSideInputBounded(Map<Instant, String> expectedSideInputValues) {
      super(expectedSideInputValues);
    }
  }

  @DoFn.UnboundedPerElement
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

  @Test
  public void testSideInputBounded() {
    testSideInput(IsBounded.BOUNDED);
  }

  @Test
  public void testSideInputUnbounded() {
    testSideInput(IsBounded.UNBOUNDED);
  }

  private void testSideInput(IsBounded bounded) {
    final PCollectionView<String> sideInput =
        p.apply("side input", Create.of("foo")).apply(View.asSingleton());

    final PCollection<String> res =
        p.apply("input", Create.of(0, 1, 2))
            .apply(
                ParDo.of(
                        sdfWithSideInput(
                            bounded,
                            Collections.singletonMap(GlobalWindow.TIMESTAMP_MIN_VALUE, "foo")))
                    .withSideInput("sideInput", sideInput));

    PAssert.that(res).containsInAnyOrder(Arrays.asList("foo:0", "foo:1", "foo:2"));

    p.run();
  }

  @Test
  public void testWindowedSideInputBounded() {
    testWindowedSideInput(IsBounded.BOUNDED);
  }

  @Test
  public void testWindowedSideInputUnbounded() {
    testWindowedSideInput(IsBounded.UNBOUNDED);
  }

  private void testWindowedSideInput(IsBounded bounded) {
    final PCollection<Integer> mainInput =
        p.apply(
                "main",
                Create.timestamped(
                    TimestampedValue.of(0, new Instant(0)),
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(2, new Instant(2)),
                    TimestampedValue.of(3, new Instant(3)),
                    TimestampedValue.of(4, new Instant(4)),
                    TimestampedValue.of(5, new Instant(5)),
                    TimestampedValue.of(6, new Instant(6)),
                    TimestampedValue.of(7, new Instant(7))))
            .apply("window 2", Window.into(FixedWindows.of(Duration.millis(2))));

    final PCollectionView<String> sideInput =
        p.apply(
                "side",
                Create.timestamped(
                    TimestampedValue.of("a", new Instant(0)),
                    TimestampedValue.of("b", new Instant(4))))
            .apply("window 4", Window.into(FixedWindows.of(Duration.millis(4))))
            .apply("singleton", View.asSingleton());

    final PCollection<String> res =
        mainInput.apply(
            ParDo.of(
                    sdfWithSideInput(
                        bounded,
                        ImmutableMap.<Instant, String>builder()
                            .put(new Instant(0), "a")
                            .put(new Instant(1), "a")
                            .put(new Instant(2), "a")
                            .put(new Instant(3), "a")
                            .put(new Instant(4), "b")
                            .put(new Instant(5), "b")
                            .put(new Instant(6), "b")
                            .put(new Instant(7), "b")
                            .build()))
                .withSideInput("sideInput", sideInput));

    PAssert.that(res).containsInAnyOrder("a:0", "a:1", "a:2", "a:3", "b:4", "b:5", "b:6", "b:7");

    p.run();
  }

  private static class SDFWithMultipleOutputsPerBlockAndSideInputBase
      extends DoFn<Integer, KV<String, Integer>> {
    private static final int MAX_INDEX = 98765;
    private final Map<Instant, String> expectedSideInputValues;
    private final int numClaimsPerCall;

    SDFWithMultipleOutputsPerBlockAndSideInputBase(
        Map<Instant, String> expectedSideInputValues, int numClaimsPerCall) {
      this.expectedSideInputValues = expectedSideInputValues;
      this.numClaimsPerCall = numClaimsPerCall;
    }

    private static int snapToNextBlock(int index, int[] blockStarts) {
      for (int i = 1; i < blockStarts.length; ++i) {
        if (index > blockStarts[i - 1] && index <= blockStarts[i]) {
          return i;
        }
      }

      throw new IllegalStateException("Shouldn't get here");
    }

    @ProcessElement
    public ProcessContinuation process(
        ProcessContext c,
        RestrictionTracker<OffsetRange, Long> tracker,
        @SideInput("sideInput") String sideInput) {
      int[] blockStarts = {-1, 0, 12, 123, 1234, 12345, 34567, MAX_INDEX};
      final int trueStarts =
          snapToNextBlock((int) tracker.currentRestriction().getFrom(), blockStarts);

      for (int i = trueStarts, numIterations = 1;
          tracker.tryClaim((long) blockStarts[i]);
          ++i, ++numIterations) {
        for (int index = blockStarts[i]; index < blockStarts[i + 1]; ++index) {
          c.output(KV.of(sideInput + ":" + c.element(), index));
        }
        if (numIterations == numClaimsPerCall) {
          return ProcessContinuation.resume();
        }
      }

      return ProcessContinuation.stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction(
        @SideInput("sideInput") String sideInput, @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new OffsetRange(0, MAX_INDEX);
    }

    @GetInitialWatermarkEstimatorState
    public Instant getInitialWatermarkEstimatorState(
        @SideInput("sideInput") String sideInput, @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return timestamp;
    }

    @GetSize
    public double getSize(
        @Restriction OffsetRange range,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return range.getTo() - range.getFrom();
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange restriction,
        OutputReceiver<OffsetRange> splitReceiver,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      splitReceiver.output(restriction);
    }

    @TruncateRestriction
    public RestrictionTracker.TruncateResult<OffsetRange> truncate(
        @Restriction OffsetRange restriction,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return RestrictionTracker.TruncateResult.of(restriction);
    }

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(
        @Restriction OffsetRange restriction,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new OffsetRangeTracker(restriction);
    }

    @NewWatermarkEstimator
    public WatermarkEstimator<Instant> newWatermarkEstimator(
        @WatermarkEstimatorState Instant watermarkEstimatorState,
        @SideInput("sideInput") String sideInput,
        @Timestamp Instant timestamp) {
      assertThat(expectedSideInputValues, hasEntry(timestamp, sideInput));
      return new WatermarkEstimators.MonotonicallyIncreasing(timestamp);
    }
  }

  @DoFn.BoundedPerElement
  private static class SDFWithMultipleOutputsPerBlockAndSideInputBounded
      extends SDFWithMultipleOutputsPerBlockAndSideInputBase {
    private SDFWithMultipleOutputsPerBlockAndSideInputBounded(
        Map<Instant, String> expectedSideInputValues, int numClaimsPerCall) {
      super(expectedSideInputValues, numClaimsPerCall);
    }
  }

  @DoFn.UnboundedPerElement
  private static class SDFWithMultipleOutputsPerBlockAndSideInputUnbounded
      extends SDFWithMultipleOutputsPerBlockAndSideInputBase {
    private SDFWithMultipleOutputsPerBlockAndSideInputUnbounded(
        Map<Instant, String> expectedSideInputValues, int numClaimsPerCall) {
      super(expectedSideInputValues, numClaimsPerCall);
    }
  }

  private static SDFWithMultipleOutputsPerBlockAndSideInputBase
      sdfWithMultipleOutputsPerBlockAndSideInput(
          IsBounded bounded, Map<Instant, String> expectedSideInputValues, int numClaimsPerCall) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithMultipleOutputsPerBlockAndSideInputBounded(
            expectedSideInputValues, numClaimsPerCall)
        : new SDFWithMultipleOutputsPerBlockAndSideInputUnbounded(
            expectedSideInputValues, numClaimsPerCall);
  }

  @Test
  public void testWindowedSideInputWithCheckpointsBounded() {
    testWindowedSideInputWithCheckpoints(IsBounded.BOUNDED);
  }

  @Test
  public void testWindowedSideInputWithCheckpointsUnbounded() {
    testWindowedSideInputWithCheckpoints(IsBounded.UNBOUNDED);
  }

  private void testWindowedSideInputWithCheckpoints(IsBounded bounded) {
    final PCollection<Integer> mainInput =
        p.apply(
                "main",
                Create.timestamped(
                    TimestampedValue.of(0, new Instant(0)),
                    TimestampedValue.of(1, new Instant(1)),
                    TimestampedValue.of(2, new Instant(2)),
                    TimestampedValue.of(3, new Instant(3))))
            .apply("window 1", Window.into(FixedWindows.of(Duration.millis(1))));

    final PCollectionView<String> sideInput =
        p.apply(
                "side",
                Create.timestamped(
                    TimestampedValue.of("a", new Instant(0)),
                    TimestampedValue.of("b", new Instant(2))))
            .apply("window 2", Window.into(FixedWindows.of(Duration.millis(2))))
            .apply("singleton", View.asSingleton());

    final PCollection<KV<String, Integer>> res =
        mainInput.apply(
            ParDo.of(
                    sdfWithMultipleOutputsPerBlockAndSideInput(
                        bounded,
                        ImmutableMap.<Instant, String>builder()
                            .put(new Instant(0), "a")
                            .put(new Instant(1), "a")
                            .put(new Instant(2), "b")
                            .put(new Instant(3), "b")
                            .build(),
                        3))
                .withSideInput("sideInput", sideInput));

    final PCollection<KV<String, Iterable<Integer>>> grouped = res.apply(GroupByKey.create());

    PAssert.that(grouped.apply(Keys.create())).containsInAnyOrder("a:0", "a:1", "b:2", "b:3");
    PAssert.that(grouped)
        .satisfies(
            input -> {
              final List<Integer> expected = new ArrayList<>();

              for (int i = 0; i < SDFWithMultipleOutputsPerBlockAndSideInputBase.MAX_INDEX; ++i) {
                expected.add(i);
              }

              for (KV<String, Iterable<Integer>> kv : input) {
                assertEquals(expected, Ordering.natural().sortedCopy(kv.getValue()));
              }
              return null;
            });

    p.run();
  }

  private static class SDFWithAdditionalOutputBase extends DoFn<Integer, String> {
    private final TupleTag<String> additionalOutput;

    private SDFWithAdditionalOutputBase(TupleTag<String> additionalOutput) {
      this.additionalOutput = additionalOutput;
    }

    @ProcessElement
    public void process(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      checkState(tracker.tryClaim(tracker.currentRestriction().getFrom()));
      c.output("main:" + c.element());
      c.output(additionalOutput, "additional:" + c.element());
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      return new OffsetRange(0, 1);
    }
  }

  @DoFn.BoundedPerElement
  private static class SDFWithAdditionalOutputBounded extends SDFWithAdditionalOutputBase {
    private SDFWithAdditionalOutputBounded(TupleTag<String> additionalOutput) {
      super(additionalOutput);
    }
  }

  @DoFn.UnboundedPerElement
  private static class SDFWithAdditionalOutputUnbounded extends SDFWithAdditionalOutputBase {
    private SDFWithAdditionalOutputUnbounded(TupleTag<String> additionalOutput) {
      super(additionalOutput);
    }
  }

  private static SDFWithAdditionalOutputBase sdfWithAdditionalOutput(
      IsBounded bounded, TupleTag<String> additionalOutput) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithAdditionalOutputBounded(additionalOutput)
        : new SDFWithAdditionalOutputUnbounded(additionalOutput);
  }

  @Test
  public void testAdditionalOutputBounded() {
    testAdditionalOutput(IsBounded.BOUNDED);
  }

  @Test
  public void testAdditionalOutputUnbounded() {
    testAdditionalOutput(IsBounded.UNBOUNDED);
  }

  private void testAdditionalOutput(IsBounded bounded) {
    final TupleTag<String> mainOutputTag = new TupleTag<>("main") {};
    final TupleTag<String> additionalOutputTag = new TupleTag<>("additional") {};

    final PCollectionTuple res =
        p.apply("input", Create.of(0, 1, 2))
            .apply(
                ParDo.of(sdfWithAdditionalOutput(bounded, additionalOutputTag))
                    .withOutputTags(mainOutputTag, TupleTagList.of(additionalOutputTag)));

    PAssert.that(res.get(mainOutputTag))
        .containsInAnyOrder(Arrays.asList("main:0", "main:1", "main:2"));

    PAssert.that(res.get(additionalOutputTag))
        .containsInAnyOrder(Arrays.asList("additional:0", "additional:1", "additional:2"));

    p.run();
  }

  @Test
  public void testLateData() {
    final Instant base = Instant.now();

    final TestStream<String> stream =
        TestStream.create(StringUtf8Coder.of())
            .advanceWatermarkTo(base)
            .addElements("aa")
            .advanceWatermarkTo(base.plus(Duration.standardSeconds(5)))
            .addElements(TimestampedValue.of("bb", base.minus(Duration.standardHours(1))))
            .advanceProcessingTime(Duration.standardHours(1))
            .advanceWatermarkToInfinity();

    final PCollection<String> input =
        p.apply(stream)
            .apply(
                Window.<String>into(FixedWindows.of(Duration.standardMinutes(1)))
                    .withAllowedLateness(Duration.standardMinutes(1))
                    .discardingFiredPanes());

    final PCollection<KV<String, Integer>> afterSDF =
        input
            .apply(ParDo.of(pairStringWithIndexToLengthFn(IsBounded.BOUNDED)))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), BigEndianIntegerCoder.of()));

    final PCollection<String> nonLate = afterSDF.apply(GroupByKey.create()).apply(Keys.create());

    PAssert.that(afterSDF)
        .containsInAnyOrder(
            Arrays.asList(KV.of("aa", 0), KV.of("aa", 1), KV.of("bb", 0), KV.of("bb", 1)));

    assertEquals(afterSDF.getWindowingStrategy(), input.getWindowingStrategy());

    PAssert.that(nonLate).containsInAnyOrder("aa");

    p.run();
  }

  private static class SDFWithLifecycleBase extends DoFn<String, String> {
    private enum State {
      OUTSIDE_BUNDLE,
      INSIDE_BUNDLE,
      TORN_DOWN
    }

    private transient State state;

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      return new OffsetRange(0, 1);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction OffsetRange range, OutputReceiver<OffsetRange> receiver) {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      receiver.output(range);
    }

    @Setup
    public void setup() {
      assertEquals(null, state);
      state = State.OUTSIDE_BUNDLE;
    }

    @StartBundle
    public void startBundle() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      state = State.INSIDE_BUNDLE;
    }

    @ProcessElement
    public void processElement(ProcessContext c, RestrictionTracker<OffsetRange, Long> tracker) {
      assertEquals(State.INSIDE_BUNDLE, state);
      assertTrue(tracker.tryClaim(0L));
      c.output(c.element());
    }

    @FinishBundle
    public void finishBundle() {
      assertEquals(State.INSIDE_BUNDLE, state);
      state = State.OUTSIDE_BUNDLE;
    }

    @Teardown
    public void teardown() {
      assertEquals(State.OUTSIDE_BUNDLE, state);
      state = State.TORN_DOWN;
    }
  }

  @DoFn.BoundedPerElement
  private static class SDFWithLifecycleBounded extends SDFWithLifecycleBase {}

  @DoFn.UnboundedPerElement
  private static class SDFWithLifecycleUnbounded extends SDFWithLifecycleBase {}

  private static SDFWithLifecycleBase sdfWithLifecycle(IsBounded bounded) {
    return (bounded == IsBounded.BOUNDED)
        ? new SDFWithLifecycleBounded()
        : new SDFWithLifecycleUnbounded();
  }

  @Test
  public void testLifecycleMethodsBounded() {
    testLifecycleMethods(IsBounded.BOUNDED);
  }

  @Test
  public void testLifecycleMethodsUnbounded() {
    testLifecycleMethods(IsBounded.UNBOUNDED);
  }

  private void testLifecycleMethods(IsBounded bounded) {
    final PCollection<String> res =
        p.apply(Create.of("a", "b", "c")).apply(ParDo.of(sdfWithLifecycle(bounded)));

    PAssert.that(res).containsInAnyOrder("a", "b", "c");

    p.run();
  }

  @Test
  public void testBoundedness() {
    final TestPipeline p = TestPipeline.create();
    final PCollection<String> foo = p.apply(Create.of("foo"));
    {
      final PCollection<String> res =
          foo.apply(
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public void process(RestrictionTracker<OffsetRange, Long> tracker) {}

                    @GetInitialRestriction
                    public OffsetRange getInitialRestriction() {
                      return new OffsetRange(0, 1);
                    }
                  }));

      assertEquals(IsBounded.BOUNDED, res.isBounded());
    }
    {
      final PCollection<String> res =
          foo.apply(
              ParDo.of(
                  new DoFn<String, String>() {
                    @ProcessElement
                    public ProcessContinuation process(
                        RestrictionTracker<OffsetRange, Long> tracker) {
                      return ProcessContinuation.stop();
                    }

                    @GetInitialRestriction
                    public OffsetRange getInitialRestriction() {
                      return new OffsetRange(0, 1);
                    }
                  }));

      assertEquals(IsBounded.UNBOUNDED, res.isBounded());
    }
  }

  public static class BundleFinalizingSplittableDoFn extends DoFn<String, String> {
    private static final long MAX_ATTEMPTS = 3000;
    private static final Map<UUID, AtomicBoolean> WAS_FINALIZED = new HashMap<>();
    private final UUID uuid = UUID.randomUUID();

    @NewTracker
    public RestrictionTracker<OffsetRange, Long> newTracker(@Restriction OffsetRange restriction) {
      return new OffsetRangeTracker(restriction) {
        @Override
        public SplitResult<OffsetRange> trySplit(double fractionOfRemainder) {
          return super.trySplit(0);
        }
      };
    }

    @ProcessElement
    public ProcessContinuation process(
        @Element String element,
        OutputReceiver<String> receiver,
        RestrictionTracker<OffsetRange, Long> tracker,
        BundleFinalizer bundleFinalizer)
        throws InterruptedException {
      if (WAS_FINALIZED.computeIfAbsent(uuid, (unused) -> new AtomicBoolean()).get()) {
        tracker.tryClaim(tracker.currentRestriction().getFrom() + 1);
        receiver.output(element);

        tracker.tryClaim(Long.MAX_VALUE);
        return ProcessContinuation.stop();
      }

      if (tracker.tryClaim(tracker.currentRestriction().getFrom() + 1)) {
        bundleFinalizer.afterBundleCommit(
            Instant.now().plus(Duration.standardSeconds(MAX_ATTEMPTS)),
            () -> WAS_FINALIZED.computeIfAbsent(uuid, (unused) -> new AtomicBoolean()).set(true));
        Thread.sleep(100);
        return ProcessContinuation.resume();
      }
      return ProcessContinuation.stop();
    }

    @GetInitialRestriction
    public OffsetRange getInitialRestriction() {
      return new OffsetRange(0, MAX_ATTEMPTS);
    }
  }

  @Test
  public void testBundleFinalizationOccursOnBoundedSplittableDoFn() {
    @DoFn.BoundedPerElement
    class BoundedBundleFinalizingSplittableDoFn extends BundleFinalizingSplittableDoFn {}

    final PCollection<String> foo = p.apply(Create.of("foo"));
    final PCollection<String> res =
        foo.apply(ParDo.of(new BoundedBundleFinalizingSplittableDoFn()));
    PAssert.that(res).containsInAnyOrder("foo");

    p.run();
  }

  @Test
  public void testBundleFinalizationOccursOnUnboundedSplittableDoFn() throws Exception {
    @DoFn.UnboundedPerElement
    class UnboundedBundleFinalizingSplittableDoFn extends BundleFinalizingSplittableDoFn {}

    PCollection<String> foo = p.apply(Create.of("foo"));
    PCollection<String> res = foo.apply(ParDo.of(new UnboundedBundleFinalizingSplittableDoFn()));
    PAssert.that(res).containsInAnyOrder("foo");
    p.run();
  }
}
