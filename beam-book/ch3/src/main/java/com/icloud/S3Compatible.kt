package com.icloud


import org.apache.beam.sdk.io.TextIO
import org.apache.beam.sdk.io.aws2.options.S3Options
import org.apache.beam.sdk.transforms.Create

object S3Compatible {

    @JvmStatic
    fun main(args: Array<String>) {
        val (pipeline, options) = PipelineUtils.from(args, S3Options::class.java)
        pipeline.apply(
            Create.of("Hello", "World")
        )
            .apply(
                TextIO.write()
                    .to("s3://test-ming-bucket/result.txt")
            )

        pipeline.run().waitUntilFinish()
    }
}