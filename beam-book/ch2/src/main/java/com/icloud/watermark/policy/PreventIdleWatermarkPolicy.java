package com.icloud.watermark.policy;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.io.kafka.TimestampPolicy;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.joda.time.Duration;
import org.joda.time.Instant;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * The Watermark policy for prevent idle status topic partition.
 * <p>
 * based on {@link org.apache.beam.sdk.io.kafka.CustomTimestampPolicyWithLimitedDelay}
 *
 * @param <K>
 * @param <V>
 */
public class PreventIdleWatermarkPolicy<K, V>
        extends TimestampPolicy<K, V> {

    private final Duration maxDelay;
    private final SerializableFunction<KafkaRecord<K, V>, Instant> timestampFunction;
    private Instant maxEventTimestamp;
    private final boolean preventIdleTopicPartition;

    public static <K, V> PreventIdleWatermarkPolicy<K, V> of(
            SerializableFunction<KafkaRecord<K, V>, Instant> timestampFunction,
            Duration maxDelay,
            Optional<Instant> previousWatermark,
            boolean preventIdleTopicPartition
    ) {
        return new PreventIdleWatermarkPolicy<>(timestampFunction, maxDelay, previousWatermark, preventIdleTopicPartition);
    }

    private PreventIdleWatermarkPolicy(
            SerializableFunction<KafkaRecord<K, V>, Instant> timestampFunction,
            Duration maxDelay,
            Optional<Instant> previousWatermark,
            boolean preventIdleTopicPartition
    ) {
        this.maxDelay = maxDelay;
        this.timestampFunction = timestampFunction;
        this.preventIdleTopicPartition = preventIdleTopicPartition;
        this.maxEventTimestamp =
                previousWatermark.orElse(BoundedWindow.TIMESTAMP_MIN_VALUE)
                        .plus(maxDelay);
    }

    @Override
    public Instant getTimestampForRecord(
            PartitionContext ctx,
            KafkaRecord<K, V> record) {
        final Instant timestamp = this.timestampFunction.apply(record);
        checkNotNull(timestamp, "timestamp");
        if (timestamp.isAfter(this.maxEventTimestamp)) {
            this.maxEventTimestamp = timestamp;
        }
        return timestamp;
    }

    @Override
    public Instant getWatermark(PartitionContext ctx) {
        Instant now = Instant.now();
        return this.getWatermark(ctx, now);
    }

    private Instant getWatermark(PartitionContext ctx, Instant now) {
        if (maxEventTimestamp.isAfter(now)) {
            return now.minus(maxDelay);
        }
        if (ctx.getMessageBacklog() == 0
            && ctx.getBacklogCheckTime().minus(maxDelay).isAfter(maxEventTimestamp)) {
            if (maxEventTimestamp.getMillis() > 0 || this.preventIdleTopicPartition) {
                return ctx.getBacklogCheckTime().minus(maxDelay);
            }
        }
        return maxEventTimestamp.minus(maxDelay);
    }
}
