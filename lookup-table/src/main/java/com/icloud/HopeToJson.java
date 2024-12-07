package com.icloud;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;

public class HopeToJson<InputT>
    extends PTransform<@NonNull PCollection<InputT>, @NonNull PCollection<String>> {

  public static <InputT> HopeToJson<InputT> of() {
    return new HopeToJson<>();
  }

  @Override
  public PCollection<String> expand(PCollection<InputT> input) {
    return input.apply(
        ParDo.of(
            new DoFn<InputT, String>() {

              private transient ObjectMapper mapper;

              @Setup
              public void setup() {
                this.mapper = new ObjectMapper();
              }

              @ProcessElement
              public void process(@Element InputT element, OutputReceiver<String> output)
                  throws JsonProcessingException {
                final String jsonString = this.mapper.writeValueAsString(element);
                output.output(jsonString);
              }

              @Teardown
              public void teardown() {
                if (this.mapper != null) {
                  this.mapper = null;
                }
              }
            }));
  }
}
