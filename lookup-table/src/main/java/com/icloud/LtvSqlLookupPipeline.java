package com.icloud;

import com.google.api.services.bigquery.model.TableRow;
import com.icloud.fn.JsonToTransactionFn;
import com.icloud.fn.UpdateUserStateWithLookupFn;
import com.icloud.model.UserTransaction;
import com.icloud.options.LtvSqlLookupPipelineOptions;
import com.icloud.utils.MySqlConnection;
import com.icloud.utils.TableRowParseUtils;
import java.sql.PreparedStatement;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Wait;
import org.apache.beam.sdk.values.PCollection;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LtvSqlLookupPipeline {

  private static final Logger LOG = LoggerFactory.getLogger(LtvSqlLookupPipeline.class);

  public static void main(String[] args) throws NoSuchSchemaException {
    final Pipeline pipeline = PipelineUtils.create(args, LtvSqlLookupPipelineOptions.class);

    final Coder<UserTransaction> userTransactionCoder =
        pipeline.getSchemaRegistry().getSchemaCoder(UserTransaction.class);
    final LtvSqlLookupPipelineOptions options =
        pipeline.getOptions().as(LtvSqlLookupPipelineOptions.class);
    options.setStreaming(true);

    final PCollection<Void> initLookupTableLock =
        pipeline
            .apply(
                "Read Initial State from BigQuery",
                BigQueryIO.readTableRows()
                    .fromQuery(
                        String.format(
                            "SELECT uid, ROUND(SUM(price), 2) AS total, COUNT(uid) AS n "
                                + "FROM `%s` WHERE uid IS NOT NULL "
                                + "GROUP BY uid",
                            options.getTransactionsTable()))
                    .usingStandardSql())
            .apply(
                "Write to MySQL Lookup table",
                JdbcIO.<TableRow>write()
                    .withDataSourceConfiguration(
                        MySqlConnection.getDatasourceConfig(
                            options.getCloudSqlDb(),
                            options.getCloudSqlInstanceConnectionName(),
                            options.getCloudSqlUsername(),
                            options.getCloudSqlPassword()))
                    .withStatement(
                        String.format(
                            "REPLACE INTO %s (uid, total, cnt) VALUES (?, ?, ?)",
                            options.getCloudSqlTable()))
                    .withPreparedStatementSetter(
                        (TableRow row, PreparedStatement preparedStatement) -> {
                          @NonNull final String uid = TableRowParseUtils.getStringValue(row, "uid");
                          final double total = TableRowParseUtils.getDoubleValue(row, "total");
                          final long cnt = TableRowParseUtils.getLongValue(row, "cnt");

                          preparedStatement.setString(1, uid);
                          preparedStatement.setDouble(2, total);
                          preparedStatement.setLong(3, cnt);
                        })
                    .withRetryConfiguration(
                        JdbcIO.RetryConfiguration.create(
                            3, Duration.standardMinutes(3L), Duration.standardMinutes(3L)))
                    .withResults());

    pipeline
        .apply(
            "Read raw Pubsub messages",
            PubsubIO.readStrings().fromSubscription(options.getInputSubscription()))
        .apply("Wait for init Lookup Table", Wait.on(initLookupTableLock))
        .apply("Json to Transaction", ParDo.of(new JsonToTransactionFn()))
        .setCoder(KvCoder.of(StringUtf8Coder.of(), userTransactionCoder))
        .apply("Update UserState", ParDo.of(new UpdateUserStateWithLookupFn()))
        .apply("Transaction to Json", HopeToJson.of())
        .apply("Transaction to Pubsub", PubsubIO.writeStrings().to(options.getOutputTopic()));

    pipeline.run().waitUntilFinish();
  }
}
