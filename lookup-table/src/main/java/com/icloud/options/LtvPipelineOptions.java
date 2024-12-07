package com.icloud.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation;

public interface LtvPipelineOptions extends StreamingOptions {
  /** PubSub subscription topic */
  @Validation.Required
  @Description("PubSub subscription to read")
  String getInputSubscription();

  void setInputSubscription(String value);

  @Validation.Required
  @Description("PubSub topic to push")
  String getOutputTopic();

  void setOutputTopic(String value);

  @Validation.Required
  @Description("Transactions table")
  String getTransactionsTable();

  void setTransactionsTable(String value);
}
