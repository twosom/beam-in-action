package com.icloud;

import org.apache.beam.sdk.values.TimestampedValue;
import org.joda.time.Instant;

public class TestUtils {
    public static <T> TimestampedValue<T> timestampedValue(T t, Instant instant) {
        return TimestampedValue.of(t, instant);
    }
}
