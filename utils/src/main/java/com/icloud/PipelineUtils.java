package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;

import static com.icloud.OptionUtils.createOption;

public class PipelineUtils {

    public static Pipeline create() {
        return create(new String[]{});
    }

    public static Pipeline create(String[] args) {
        return create(args, null);
    }

    public static <T extends PipelineOptions> Pipeline create(String[] args, Class<T> optionClass) {
        return createPipeline(createOption(args, optionClass));
    }

    private static Pipeline createPipeline(PipelineOptions option) {
        return Pipeline.create(option);
    }

}
