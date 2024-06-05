package com.icloud;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface CommonKafkaOptions
        extends PipelineOptions {

    @Validation.Required
    @Description("Bootstrap server for using kafka topic")
    String getBootstrapServer();

    void setBootstrapServer(String value);

    @Validation.Required
    @Description("name of topic for input data")
    String getInputTopic();

    void setInputTopic(String value);

    @Validation.Required
    @Description("name of topic for output data")
    String getOutputTopic();

    void setOutputTopic(String value);
}