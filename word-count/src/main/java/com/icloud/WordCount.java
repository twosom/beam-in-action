package com.icloud;

import com.google.common.annotations.VisibleForTesting;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

import static com.icloud.OptionUtils.createOption;

public class WordCount {
    public interface WordCountOptions extends PipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */
        @Description("Path of the file to read from")
        @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
        String getInputFile();

        void setInputFile(String value);

        /**
         * Set this required option to specify where to write the output.
         */
        @Description("Path of the file to write to")
        @Validation.Required
        String getOutput();

        void setOutput(String value);
    }

    @VisibleForTesting
    static class ExtractWordsFn
            extends DoFn<String, String> {

        public static final String TOKENIZER_PATTERN = "[^\\p{L}]+";
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

    @VisibleForTesting
    static class FormatAsTextFn
            extends SimpleFunction<KV<String, Long>, String> {

        @Override
        public String apply(KV<String, Long> input) {
            return input.getKey() + ": " + input.getValue();
        }
    }

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
}
