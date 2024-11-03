package com.icloud;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;

import java.math.BigDecimal;
import java.math.MathContext;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HopeOffsetRangeTracker extends RestrictionTracker<HopeOffsetRange, Long>
    implements HasProgress {

  private static final Logger LOG = LoggerFactory.getLogger(HopeOffsetRangeTracker.class);
  protected HopeOffsetRange range;

  /** 마지막으로 claim 성공한 오프셋 */
  protected @Nullable Long lastClaimedOffset = null;

  /** 마지막으로 시도된 오프셋 */
  protected @Nullable Long lastAttemptedOffset = null;

  public HopeOffsetRangeTracker(final HopeOffsetRange range) {
    this.range = checkNotNull(range);
  }

  @Override
  public HopeOffsetRange currentRestriction() {
    return this.range;
  }

  @Override
  public SplitResult<HopeOffsetRange> trySplit(double fractionOfRemainder) {
    // 아직 시도한 offset이 없다면 range의 from에서 1을 뺀 값을
    // 이미 시도했다면 시도한 값을 사용
    final BigDecimal current =
        (this.lastAttemptedOffset == null)
            ? BigDecimal.valueOf(this.range.getFrom())
                .subtract(BigDecimal.ONE, MathContext.DECIMAL128)
            : BigDecimal.valueOf(this.lastAttemptedOffset);
    // splitPosition = current + MAX((range.to - current) * fractionOfRemainder, 1)
    final BigDecimal splitPosition;
    if (fractionOfRemainder == .0) {
      splitPosition = current.add(BigDecimal.ONE);
    } else {
      splitPosition =
          current.add(
              BigDecimal.valueOf(range.getTo())
                  .subtract(current, MathContext.DECIMAL128)
                  .multiply(BigDecimal.valueOf(fractionOfRemainder), MathContext.DECIMAL128)
                  .max(BigDecimal.ONE),
              MathContext.DECIMAL128);
    }
    LOG.info(
        "range.from = {}, splitPosition = {}, range.to = {}",
        this.range.getFrom(),
        splitPosition,
        this.range.getTo());
    /*ignore the decimal point*/
    long split = splitPosition.longValue();
    if (split >= range.getTo()) {
      LOG.info(
          "splitPosition {} is greater than or equals with range.to, so do not split {}",
          split,
          range.getTo());
      return null;
    }

    final HopeOffsetRange residual = HopeOffsetRange.of(split, this.range.getTo());
    // TODO
    // why overwrite this current restriction?
    this.range = HopeOffsetRange.of(range.getFrom(), split);
    return SplitResult.of(this.range, residual);
  }

  @Override
  public boolean tryClaim(Long position) {
    LOG.info("tryClaim called with value {}", position);
    checkArgument(
        this.lastAttemptedOffset == null || position > this.lastAttemptedOffset,
        "Trying to claim offset %s while last attempted was %s",
        position,
        this.lastAttemptedOffset);
    checkArgument(
        position >= this.range.getFrom(),
        "Trying to claim offset %s before start of the range %s",
        position,
        this.range);
    this.lastAttemptedOffset = position;
    if (position >= this.range.getTo()) {
      LOG.info("Trying to claim offset {} but range limit is {}", position, this.range.getTo());
      return false;
    }
    this.lastClaimedOffset = position;
    return true;
  }

  @Override
  public void checkDone() throws IllegalStateException {
    if (this.range.getFrom() == this.range.getTo()) {
      LOG.info("range.from and range.to is same");
      return;
    }

    checkState(
        this.lastAttemptedOffset != null,
        "Last attempted offset should not be null. No work was claimed in non-empty range %s.",
        this.range);

    checkState(
        this.lastAttemptedOffset >= this.range.getTo() - 1,
        "Last attempted offset was %s in range %s, claiming work in [%s, %s) was not attempted.",
        this.lastAttemptedOffset,
        this.range,
        this.lastAttemptedOffset + 1,
        this.range.getTo());
  }

  @NonNull
  @Override
  public IsBounded isBounded() {
    return IsBounded.BOUNDED;
  }

  @NonNull
  @Override
  public Progress getProgress() {
    if (this.lastAttemptedOffset == null) {
      return Progress.from(
          0,
          BigDecimal.valueOf(this.range.getTo())
              .subtract(BigDecimal.valueOf(this.range.getFrom()), MathContext.DECIMAL128)
              .doubleValue());
    }

    final BigDecimal workRemaining =
        BigDecimal.valueOf(this.range.getTo())
            .subtract(BigDecimal.valueOf(this.lastAttemptedOffset), MathContext.DECIMAL128)
            .max(BigDecimal.ZERO);

    final BigDecimal totalWork =
        BigDecimal.valueOf(this.range.getTo())
            .subtract(BigDecimal.valueOf(this.range.getFrom()), MathContext.DECIMAL128);

    return Progress.from(
        totalWork.subtract(workRemaining, MathContext.DECIMAL128).doubleValue(),
        workRemaining.doubleValue());
  }
}
