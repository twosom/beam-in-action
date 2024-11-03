package com.icloud;

import static com.google.common.base.Preconditions.*;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.checkerframework.checker.nullness.qual.NonNull;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class HopeOffsetRange
    implements HasDefaultTracker<
        @NonNull HopeOffsetRange, @NonNull RestrictionTracker<HopeOffsetRange, Long>> {

  public static HopeOffsetRange of(long from, long to) {
    checkArgument(from <= to, "Malformed range [%s, %s)", from, to);
    return new AutoValue_HopeOffsetRange.Builder().setFrom(from).setTo(to).build();
  }

  public abstract long getFrom();

  public abstract long getTo();

  @Override
  public String toString() {
    return "[" + getFrom() + ", " + getTo() + ')';
  }

  @NonNull
  @Override
  public RestrictionTracker<HopeOffsetRange, Long> newTracker() {
    return new HopeOffsetRangeTracker(this);
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFrom(long from);

    abstract Builder setTo(long to);

    abstract HopeOffsetRange build();
  }
}
