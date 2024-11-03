package com.icloud;

import static org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.*;
import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.math.MathContext;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.junit.jupiter.api.Test;

class HopeOffsetRangeTrackerTest {

  @Test
  void testIllegalInitialization() {
    assertThrows(NullPointerException.class, () -> new HopeOffsetRangeTracker(null));
  }

  @Test
  void testTryClaim() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    assertEquals(range, tracker.currentRestriction());
    assertTrue(tracker.tryClaim(100L));
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(199L));
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(198L));
    assertEquals("Trying to claim offset 198 while last attempted was 199", exception.getMessage());

    assertFalse(tracker.tryClaim(200L));
  }

  @Test
  void testCheckpointUnstarted() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    final SplitResult<HopeOffsetRange> splitResult = tracker.trySplit(.0);

    assertEquals(HopeOffsetRange.of(100, 100), splitResult.getPrimary());
    assertEquals(HopeOffsetRange.of(100, 200), splitResult.getResidual());
    assertDoesNotThrow(tracker::checkDone);
  }

  @Test
  void testCheckpointJustStarted() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    assertTrue(tracker.tryClaim(100L));
    /*
     current = 100
     range.from = 100
     range.to = 200
     splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPos = 100 + MAX(1, ((200 - 100) * 0)) = 101
    */

    /*
     primary = [100, 101)
     residual = [101, 200)
    */
    final SplitResult<HopeOffsetRange> split = tracker.trySplit(.0);

    final HopeOffsetRange checkpoint /*TODO why this name is `checkpoint`*/ = split.getResidual();
    assertEquals(HopeOffsetRange.of(100, 101), tracker.currentRestriction());
    assertEquals(HopeOffsetRange.of(101, 200), checkpoint);
    assertDoesNotThrow(tracker::checkDone);
  }

  @Test
  void testCheckpointRegular() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    final SplitResult<HopeOffsetRange> split = tracker.trySplit(.0);
    /*
      current = 110
      range.from = 100
      range.to = 200
      splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
      splitPos = 110 + MAX(1, ((200 - 110) * 0)) = 111
    */
    final HopeOffsetRange checkpoint = split.getResidual();
    assertEquals(HopeOffsetRange.of(100, 111), tracker.currentRestriction());
    assertEquals(HopeOffsetRange.of(111, 200), checkpoint);

    assertDoesNotThrow(tracker::checkDone);
  }

  @Test
  void testCheckpointClaimedLast() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    assertTrue(tracker.tryClaim(199L));
    @Nullable final SplitResult<HopeOffsetRange> split = tracker.trySplit(.0);

    /*
     current = 199
     range.from = 100
     range.to = 200
     splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPos = 199 + MAX(1, ((200 - 199) * 0)) = 200
    */
    assertEquals(HopeOffsetRange.of(100, 200), tracker.currentRestriction());
    assertNull(split);
    assertDoesNotThrow(tracker::checkDone);
  }

  @Test
  void testCheckpointAfterFailedClaim() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    assertTrue(tracker.tryClaim(160L));
    assertFalse(tracker.tryClaim(240L));
    @Nullable final SplitResult<HopeOffsetRange> split = tracker.trySplit(.0);
    assertNull(split);
    assertEquals(HopeOffsetRange.of(100, 200), tracker.currentRestriction());
    assertDoesNotThrow(tracker::checkDone);
  }

  @Test
  void testTrySplit() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    assertTrue(tracker.tryClaim(100L));
    /*
     current = 100
     range.from = 100
     range.to = 200
     splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPos = 100 + MAX(1, ((200 - 100) * 0.509)) = 100 + 50.9 = 150.9
    */

    /*
     primary = [0, 150)
     residual = [150, 200)
    */
    @Nullable SplitResult<HopeOffsetRange> split = tracker.trySplit(0.509);
    assertNotNull(split);
    assertEquals(HopeOffsetRange.of(100, 150), split.getPrimary());
    assertEquals(HopeOffsetRange.of(150, 200), split.getResidual());

    /*
      current = 100
      range.from = 100
      range.to = 150
      splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
      splitPos = 100 + MAX(1, ((150 - 100) * 1)) = 100 + 50 = 150
    */
    split = tracker.trySplit(1);
    assertNull(split);
  }

  @Test
  void testSplitAtEmptyRange() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 100);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    /*
     current = 99
     range.from = 100
     range.to = 100
     splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPos = 99 + MAX(1, ((100 - 99) * 0)) = 100
    */
    assertNull(tracker.trySplit(.0));

    /*
     current = 99
     range.from = 100
     range.to = 100
     splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPos = 99 + MAX(1, ((100 - 99) * 0.1)) = 100
    */
    assertNull(tracker.trySplit(0.1));

    /*
     current = 99
     range.from = 100
     range.to = 100
     splitPos = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPos = 99 + MAX(1, ((100 - 99) * 1)) = 100
    */
    assertNull(tracker.trySplit(1));
  }

  @Test
  void testNonMonotonicClaim() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    assertTrue(tracker.tryClaim(105L));
    assertTrue(tracker.tryClaim(110L));
    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(103L));
    assertEquals("Trying to claim offset 103 while last attempted was 110", exception.getMessage());
  }

  @Test
  void testClaimsBeforeStartOfRange() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    final IllegalArgumentException exception =
        assertThrows(IllegalArgumentException.class, () -> tracker.tryClaim(90L));

    assertEquals(
        "Trying to claim offset 90 before start of the range [100, 200)", exception.getMessage());
  }

  @Test
  void testDoneBeforeClaim() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, tracker::checkDone);

    assertEquals(
        "Last attempted offset should not be null. No work was claimed in non-empty range [100, 200).",
        exception.getMessage());
  }

  @Test
  void testCheckDoneAfterTryClaimPastEndOfRange() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));
    assertFalse(tracker.tryClaim(220L));

    assertDoesNotThrow(tracker::checkDone);
    assertEquals(220L, tracker.lastAttemptedOffset);
  }

  @Test
  void testCheckDoneAfterTryClaimAtEndOfRange() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));
    assertFalse(tracker.tryClaim(200L));
    assertDoesNotThrow(tracker::checkDone);
  }

  @Test
  void testCheckDoneAfterTryClaimRightBeforeEndOfRange() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));
    assertTrue(tracker.tryClaim(199L));
    assertDoesNotThrow(tracker::checkDone);
  }

  @Test
  void testCheckDoneWhenNotDone() {
    final HopeOffsetRange range = HopeOffsetRange.of(100, 200);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    assertTrue(tracker.tryClaim(150L));
    assertTrue(tracker.tryClaim(175L));

    final IllegalStateException exception =
        assertThrows(IllegalStateException.class, tracker::checkDone);

    assertEquals(
        "Last attempted offset was 175 in range [100, 200), claiming work in [176, 200) was not attempted.",
        exception.getMessage());
  }

  @Test
  void testBacklogUnstarted() {
    HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(HopeOffsetRange.of(0, 200));

    /*
     range.from = 0
     range.to = 200

     progress = 0, range.to - range.from
     progress = 0, 200
    */
    Progress progress = tracker.getProgress();
    assertEquals(0, progress.getWorkCompleted(), .001);
    assertEquals(200, progress.getWorkRemaining(), .001);

    tracker = new HopeOffsetRangeTracker(HopeOffsetRange.of(100, 200));

    /*
     range.from = 100
     range.to = 200

     progress = 0, range.to - range.from
     progress = 0, 100
    */
    progress = tracker.getProgress();
    assertEquals(0, progress.getWorkCompleted(), .001);
    assertEquals(100, progress.getWorkRemaining(), .001);
  }

  @Test
  void testBacklogFinished() {
    HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(HopeOffsetRange.of(0, 200));
    assertFalse(tracker.tryClaim(300L));
    assertEquals(300L, tracker.lastAttemptedOffset);

    /*
     lastAttemptedOffset = 300
     range.from = 0
     range.to = 200

     workRemaining = MAX(0, range.to - lastAttemptedOffset)
     workRemaining = MAX(0, 200 - 300) = 0

     totalWork = range.to - range.from
     totalWork = 200

     workCompleted = totalWork - workRemaining
     workCompleted = 200 - 0 = 200
    */
    Progress progress = tracker.getProgress();

    assertEquals(200, progress.getWorkCompleted(), .001);
    assertEquals(0, progress.getWorkRemaining(), .001);

    tracker = new HopeOffsetRangeTracker(HopeOffsetRange.of(100, 200));
    assertFalse(tracker.tryClaim(300L));
    assertEquals(300L, tracker.lastAttemptedOffset);

    /*
     lastAttemptedOffset = 300
     range.from = 100
     range.to = 200

     workRemaining = MAX(0, range.to - lastAttemptedOffset)
     workRemaining = 0

     totalWork = range.to - range.from
     totalWork = 100

     workCompleted = totalWork - workRemaining
     workCompleted = 100
    */
    progress = tracker.getProgress();
    assertEquals(100, progress.getWorkCompleted(), .001);
    assertEquals(0, progress.getWorkRemaining(), .001);
  }

  @Test
  void testBacklogPartiallyCompleted() {
    HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(HopeOffsetRange.of(0, 200));
    assertTrue(tracker.tryClaim(150L));
    assertEquals(150L, tracker.lastAttemptedOffset);
    assertEquals(150L, tracker.lastClaimedOffset);

    /*
     lastAttemptedOffset = 150
     range.from = 0
     range.to = 200

     workRemaining = MAX(0, range.to - lastAttemptedOffset)
     workRemaining = 50

     totalWork = range.to - range.from
     totalWork = 200

     workCompleted = totalWork - workRemaining
     workCompleted = 150
    */
    Progress progress = tracker.getProgress();
    assertEquals(150, progress.getWorkCompleted(), .001);
    assertEquals(50, progress.getWorkRemaining(), .001);

    tracker = new HopeOffsetRangeTracker(HopeOffsetRange.of(100, 200));
    tracker.tryClaim(150L);
    assertEquals(150L, tracker.lastAttemptedOffset);
    assertEquals(150L, tracker.lastClaimedOffset);

    /*
     lastAttemptedOffset = 150
     range.from = 100
     range.to = 200

     workRemaining = MAX(0, range.to - lastAttemptedOffset)
     workRemaining = 50

     totalWork = range.to - range.from
     totalWork = 100

     workCompleted = totalWork - workRemaining
     workCompleted = 50
    */
    progress = tracker.getProgress();
    assertEquals(50, progress.getWorkCompleted(), .001);
    assertEquals(50, progress.getWorkRemaining(), .001);
  }

  @Test
  void testLargeRange() {
    final HopeOffsetRange range = HopeOffsetRange.of(Long.MIN_VALUE, Long.MAX_VALUE);
    final HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);

    final Progress progress = tracker.getProgress();
    assertEquals(0, progress.getWorkCompleted(), .001);

    assertEquals(
        BigDecimal.valueOf(Long.MAX_VALUE)
            .subtract(BigDecimal.valueOf(Long.MIN_VALUE), MathContext.DECIMAL128)
            .doubleValue(),
        progress.getWorkRemaining(),
        .001);

    final SplitResult<HopeOffsetRange> split = tracker.trySplit(0);

    assertEquals(HopeOffsetRange.of(Long.MIN_VALUE, Long.MIN_VALUE), split.getPrimary());
    assertEquals(HopeOffsetRange.of(Long.MIN_VALUE, Long.MAX_VALUE), split.getResidual());
  }

  @Test
  void testSmallRangeWithLargeValue() {
    final HopeOffsetRange range = HopeOffsetRange.of(123456789012345677L, 123456789012345679L);
    HopeOffsetRangeTracker tracker = new HopeOffsetRangeTracker(range);
    assertTrue(tracker.tryClaim(123456789012345677L));
    assertEquals(123456789012345677L, tracker.lastAttemptedOffset);
    assertEquals(123456789012345677L, tracker.lastClaimedOffset);

    /*
     current = 123456789012345677
     range.from = 123456789012345677
     range.to = 123456789012345679

     splitPosition = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPosition = 123456789012345676 + ((123456789012345679 - 123456789012345677) * 0.5) = 123456789012345676 + 1 = 123456789012345678
    */
    SplitResult<HopeOffsetRange> split = tracker.trySplit(.5);

    assertEquals(HopeOffsetRange.of(123456789012345677L, 123456789012345678L), split.getPrimary());
    assertEquals(HopeOffsetRange.of(123456789012345678L, 123456789012345679L), split.getResidual());

    tracker =
        new HopeOffsetRangeTracker(HopeOffsetRange.of(123456789012345681L, 123456789012345683L));
    assertTrue(tracker.tryClaim(123456789012345681L));
    assertEquals(123456789012345681L, tracker.lastAttemptedOffset);
    assertEquals(123456789012345681L, tracker.lastClaimedOffset);

    /*
     current = 123456789012345681
     range.from = 123456789012345681
     range.to = 123456789012345683

     splitPosition = current + MAX(1, ((range.to - current) * fractionOfRemainder))
     splitPosition = 123456789012345681 + ((123456789012345683 - 123456789012345681) * 0.5) = 123456789012345681 + 1 = 123456789012345682
    */
    split = tracker.trySplit(.5);

    assertEquals(HopeOffsetRange.of(123456789012345681L, 123456789012345682L), split.getPrimary());
    assertEquals(HopeOffsetRange.of(123456789012345682L, 123456789012345683L), split.getResidual());
  }
}
