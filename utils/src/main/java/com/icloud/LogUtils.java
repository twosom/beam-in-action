package com.icloud;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {
    private static final Logger LOG =
            LoggerFactory.getLogger(LogUtils.class);


    public static <T> PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>> of() {
        return new PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>>() {

            class LogDoFn extends DoFn<T, T> {
                @ProcessElement
                public void process(
                        @Element T t,
                        OutputReceiver<T> output
                ) {
                    LOG.info("{}", t);
                    output.output(t);
                }
            }

            @Override
            public PCollection<T> expand(PCollection<T> input) {
                return input.apply(ParDo.of(new LogDoFn()));
            }
        };
    }


}
