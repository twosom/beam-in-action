package com.icloud;

import static com.icloud.WordCount.ExtractWordsFn;

import com.icloud.WordCount.CountWords;
import java.util.Arrays;
import java.util.List;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

class WordCountTest {

  static final String[] WORDS_ARRAY =
      new String[] {
        "hi there", "hi", "hi sue bob",
        "hi sue", "", "bob hi"
      };
  static final List<String> WORDS = Arrays.asList(WORDS_ARRAY);
  static final String[] COUNTS_ARRAY = new String[] {"hi: 5", "there: 1", "sue: 2", "bob: 2"};
  private final Pipeline p = Pipeline.create();

  @Test
  void testExtractWordsFn() {
    final List<String> words = Arrays.asList(" some input words ", " ", " cool ", " foo ", " bar");

    final PCollection<String> output =
        p.apply(Create.of(words)).apply(ParDo.of(new ExtractWordsFn()));

    PAssert.that(output).containsInAnyOrder("some", "input", "words", "cool", "foo", "bar");

    p.run().waitUntilFinish();
  }

  @Test
  void testCountWords() {
    final PCollection<String> input = p.apply(Create.of(WORDS).withCoder(StringUtf8Coder.of()));
    final PCollection<String> output =
        input.apply(new CountWords()).apply(MapElements.via(new FormatAsTextFn()));

    PAssert.that(output).containsInAnyOrder(COUNTS_ARRAY);
    p.run().waitUntilFinish();
  }
}
