package com.icloud;

import static com.icloud.ExampleUtils.TOKENIZER_PATTERN;
import static com.icloud.OptionUtils.createOption;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

public class WordCount {

    public static void runWordCount(WordCountOptions options) {
        final Pipeline p = Pipeline.create(options);

        p.apply("ReadLines", TextIO.read().from(options.getInputFile()))
                .apply(new CountWords())
                .apply(MapElements.via(new FormatAsTextFn()))
                .apply("WriteCounts", TextIO.write().to(options.getOutput()));


        p.run().waitUntilFinish();
    }

    public static void main(String[] args) {
        final WordCountOptions options =
                createOption(args, WordCountOptions.class);
        runWordCount(options);
    }

    @VisibleForTesting
    static class ExtractWordsFn
            extends DoFn<String, String> {

        private final Counter emptyLines =
                Metrics.counter(ExtractWordsFn.class, "emptyLines");

        private final Distribution lineLenDist =
                Metrics.distribution(ExtractWordsFn.class, "lineLenDistro");

        @ProcessElement
        public void process(
                @Element String element,
                OutputReceiver<String> output
        ) {
            this.lineLenDist.update(element.length());
            if (element.trim().isEmpty()) {
                this.emptyLines.inc();
            }

            final String[] words = element.split(TOKENIZER_PATTERN, -1);
            for (String word : words) {
                if (!word.isEmpty()) {
                    output.output(word);
                }
            }
        }

    }

    @VisibleForTesting
    static class CountWords
            extends PTransform<@NonNull PCollection<String>, @NonNull PCollection<KV<String, Long>>> {

        @Override
        public PCollection<KV<String, Long>> expand(PCollection<String> lines) {
            return lines
                    .apply(ParDo.of(new ExtractWordsFn()))
                    .apply(Count.perElement());
        }

    }
}
