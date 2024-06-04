package com.icloud;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.transforms.windowing.PaneInfo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogUtils {
    private static final Logger LOG =
            LoggerFactory.getLogger(LogUtils.class);

    public static <T> PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>> of() {
        return of(false);
    }


    public static <T> PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>> of(boolean debug) {
        return new BeamLogger<>(debug);
    }


    private static class BeamLogger<T>
            extends PTransform<@NonNull PCollection<T>, @NonNull PCollection<T>> {

        private final boolean debug;

        public BeamLogger(boolean debug) {
            this.debug = debug;
        }

        class LogDoFn extends DoFn<T, T> {
            @ProcessElement
            public void process(
                    @Element T t,
                    BoundedWindow window,
                    PaneInfo paneInfo,
                    OutputReceiver<T> output
            ) {
                if (debug) {
                    LOG.info("{} {} - {}", t, paneInfo.getTiming(), window);
                } else {
                    LOG.info("{}", t);
                }

                output.output(t);
            }
        }

        @Override
        public PCollection<T> expand(PCollection<T> input) {
            return input.apply(ParDo.of(new LogDoFn()));
        }
    }
}
