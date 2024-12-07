package com.icloud;

import com.icloud.fn.JsonToTransactionFn;
import com.icloud.fn.TableRowToUserStateFn;
import com.icloud.fn.UpdateUserStateFn;
import com.icloud.model.UserState;
import com.icloud.model.UserTransaction;
import com.icloud.options.LtvPipelineOptions;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.PCollectionView;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LtvPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(LtvPipeline.class);

  public static void main(String[] args) throws NoSuchSchemaException {

    final Pipeline pipeline = PipelineUtils.create(args, LtvPipelineOptions.class);
    final LtvPipelineOptions options = pipeline.getOptions().as(LtvPipelineOptions.class);
    options.setStreaming(true);

    LOG.info("Starting pipeline...");

    final Coder<UserState> userStateCoder =
        pipeline.getSchemaRegistry().getSchemaCoder(UserState.class);
    final Coder<UserTransaction> userTransactionCoder =
        pipeline.getSchemaRegistry().getSchemaCoder(UserTransaction.class);

    final PCollectionView<Map<String, UserState>> bqInitialState =
        pipeline
            .apply(
                "Read Initial State from BigQuery",
                BigQueryIO.readTableRows()
                    .fromQuery(
                        String.format(
                            "SELECT uid, ROUND(SUM(price), 2) AS total, COUNT(uid) AS n FROM `%s` GROUP BY uid",
                            options.getTransactionsTable()))
                    .usingStandardSql())
            .apply("Table Row To UserState", ParDo.of(new TableRowToUserStateFn()))
            .setCoder(KvCoder.of(StringUtf8Coder.of(), userStateCoder))
            .apply(View.asMap());

    pipeline
        .apply(
            "Read raw PubSub messages",
            PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
        .apply("Json to Transaction transformation", ParDo.of(new JsonToTransactionFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), userTransactionCoder))
        .apply(
            "Update User State",
            ParDo.of(new UpdateUserStateFn()).withSideInput("storage", bqInitialState))
        .apply("Transaction To Json", HopeToJson.of())
        .apply("Transaction to Pubsub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    pipeline.run().waitUntilFinish();
  }
}
