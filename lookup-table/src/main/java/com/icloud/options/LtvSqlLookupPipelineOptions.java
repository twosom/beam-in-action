package com.icloud.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public interface LtvSqlLookupPipelineOptions extends CloudSqlOptions {

  /** PubSub subscription topic */
  @Required
  @Description("PubSub subscription to read")
  String getInputSubscription();

  void setInputSubscription(String value);

  @Required
  @Description("PubSub topic to push")
  String getOutputTopic();

  void setOutputTopic(String value);

  @Required
  @Description("Transactions table")
  String getTransactionsTable();

  void setTransactionsTable(String value);
}
