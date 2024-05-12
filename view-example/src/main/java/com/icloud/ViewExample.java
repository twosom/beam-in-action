package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;

public class ViewExample {
    public static void main(String[] args) {
        final Pipeline pipeline = PipelineUtils.create(args);

        final PCollection<KV<String, String>> citiesToCountries = pipeline.apply(
                "Cities and Countries",
                Create.of(
                        KV.of("Beijing", "China"),
                        KV.of("London", "United Kingdom"),
                        KV.of("San Francisco", "United States"),
                        KV.of("Singapore", "Singapore"),
                        KV.of("Sydney", "Australia")
                )
        );

        final PCollection<KV<String, String>> persons = pipeline.apply(
                "Persons",
                Create.of(
                        KV.of("Henry", "Singapore"),
                        KV.of("Jane", "San Francisco"),
                        KV.of("Lee", "Beijing"),
                        KV.of("John", "Sydney"),
                        KV.of("Alfred", "London")
                )
        );


        final PCollectionView<Map<String, String>> citiesToCountriesView =
                citiesToCountries.apply(View.asMap());

        final PCollection<KV<String, String>> output = persons.apply(
                ParDo.of(new DoFn<KV<String, String>, KV<String, String>>() {
                    @ProcessElement
                    public void process(
                            @Element KV<String, String> person,
                            ProcessContext c,
                            OutputReceiver<KV<String, String>> outputs
                    ) {
                        final Map<String, String> citiesToCountries =
                                c.sideInput(citiesToCountriesView);

                        final String city = person.getValue();
                        final String country = citiesToCountries.getOrDefault(city, "Unknown");
                        outputs.output(KV.of(person.getKey(), country));
                    }
                }).withSideInputs(citiesToCountriesView)
        );

        output.apply(LogUtils.of());


        pipeline.run().waitUntilFinish();
    }
}
