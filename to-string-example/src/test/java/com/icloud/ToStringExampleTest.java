package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ToString;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.jupiter.api.Test;

class ToStringExampleTest {

    @Test
    void testToStringPTransform() {
        final Pipeline pipeline = PipelineUtils.create();

        final PCollection<Integer> elemInput =
                pipeline.apply(Create.of(1, 2, 3, 4, 5));

        final PCollection<String> elemResult =
                elemInput.apply(ToString.elements());

        final PCollection<KV<String, String>> kvInput =
                pipeline.apply(Create.of(KV.of("key1", "value1"), KV.of("key2", "value2")));

        final PCollection<String> kvResult =
                kvInput.apply(ToString.kvs());

        PAssert.that(kvResult)
                .containsInAnyOrder("key1,value1", "key2,value2");

        PAssert.that(elemResult)
                .containsInAnyOrder("1", "2", "3", "4", "5");

        pipeline.run();
    }

}