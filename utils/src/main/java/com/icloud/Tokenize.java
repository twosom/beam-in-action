package com.icloud;

import com.google.common.base.Strings;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.transforms.FlatMapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.checkerframework.checker.nullness.qual.NonNull;

public class Tokenize
    extends PTransform<@NonNull PCollection<String>, @NonNull PCollection<String>> {

  private Tokenize() {}

  public static Tokenize of() {
    return new Tokenize();
  }

  @Override
  public PCollection<String> expand(PCollection<String> input) {
    final PCollection<String> result =
        input.apply(
            "Split to Tokens", FlatMapElements.into(TypeDescriptors.strings()).via(this::toWords));

    if (result.hasSchema()) {
      result.setSchema(
          input.getSchema(),
          input.getTypeDescriptor(),
          input.getToRowFunction(),
          input.getFromRowFunction());
    }

    return result;
  }

  private List<String> toWords(String input) {
    return Arrays.stream(input.split("\\W+"))
        .filter(e -> !Strings.isNullOrEmpty(e))
        .collect(Collectors.toList());
  }
}
