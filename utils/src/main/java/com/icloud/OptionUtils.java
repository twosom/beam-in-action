package com.icloud;

import org.apache.beam.sdk.options.ApplicationNameOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.util.common.ReflectHelpers;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class OptionUtils {
    public static <T extends PipelineOptions> PipelineOptions createOption(String[] args, Class<T> optionClass) {
        if (optionClass != null) {
            PipelineOptionsFactory.register(optionClass);
        }
        final PipelineOptionsFactory.Builder optionsBuilder = PipelineOptionsFactory.fromArgs(args)
                .withValidation();

        return withApplicationName(
                optionClass == null ?
                        optionsBuilder.create() :
                        optionsBuilder.as(optionClass)
        );
    }


    private static PipelineOptions withApplicationName(PipelineOptions options) {
        options.as(ApplicationNameOptions.class)
                .setAppName(findCallerClassName());
        return options;
    }

    private static String findCallerClassName() {
        final List<StackTraceElement> stackElements = Arrays.stream(Thread.currentThread().getStackTrace())
                .collect(Collectors.toList());

        final StackTraceElement caller = stackElements.remove(
                stackElements.size() - 1
        );

        try {
            return Class.forName(caller.getClassName(), true, ReflectHelpers.findClassLoader())
                    .getSimpleName();
        } catch (ClassNotFoundException e) {
            return "unknown";
        }
    }
}
