package com.icloud.fn;

import com.icloud.model.UserState;
import com.icloud.model.UserTransaction;
import com.icloud.options.LtvSqlLookupPipelineOptions;
import com.icloud.utils.MySqlConnection;
import com.icloud.utils.ObjectUtils;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import javax.sql.DataSource;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Distribution;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateUserStateWithLookupFn
    extends DoFn<KV<String, UserTransaction>, UserTransaction> {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateUserStateWithLookupFn.class);

  @StateId("user-state")
  private final StateSpec<@NonNull ValueState<UserState>> userStateSpec = StateSpecs.value();

  private transient DataSource dataSource;
  private String lookupDb;
  private String lookupTable;

  private static final String TEMPLATE = "SELECT * FROM %s.%s WHERE uid = \"%s\"";

  // for metrics...
  private final Distribution processingSqlQuery =
      Metrics.distribution(UpdateUserStateWithLookupFn.class, "processing_time_sql_cache");

  private final Counter startBundleCounter =
      Metrics.counter(UpdateUserStateWithLookupFn.class, "start_bundle_counter");

  @StartBundle
  public void startBundle(PipelineOptions options) {
    final LtvSqlLookupPipelineOptions ltvOptions = options.as(LtvSqlLookupPipelineOptions.class);
    LOG.info("[@StartBundle] initializing state...");

    this.dataSource =
        MySqlConnection.getInstance(
                ltvOptions.getCloudSqlDb(),
                ltvOptions.getCloudSqlInstanceConnectionName(),
                ltvOptions.getCloudSqlUsername(),
                ltvOptions.getCloudSqlPassword())
            .getDataSource();

    this.lookupDb = ltvOptions.getCloudSqlDb();
    this.lookupTable = ltvOptions.getCloudSqlTable();

    this.startBundleCounter.inc();
  }

  @ProcessElement
  public void process(
      @Element KV<String, UserTransaction> element,
      @StateId("user-state") ValueState<UserState> userState,
      OutputReceiver<UserTransaction> output) {

    final String uid = element.getKey();

    final UserTransaction userTransaction = element.getValue();

    final @Nullable UserState currentState =
        ObjectUtils.firstNonNull(userState.read(), /*Nullable*/ this.getUserStateFromLookupTable(uid));

    @NonNull Double newTotal;
    @NonNull Long newCount;
    if (currentState != null) {
      newTotal = currentState.getTotal() + userTransaction.getPrice();
      newCount = currentState.getCount() + 1;
    } else {
      newTotal = userTransaction.getPrice();
      newCount = 1L;
    }

    final UserState newState = UserState.create(newTotal, newCount);
    userState.write(newState);

    LOG.info("[@ProcessElement] user state {} updated...", uid);
    output.output(userTransaction);
  }

  private @Nullable UserState getUserStateFromLookupTable(String uid) {
    final long sqlBefore = Instant.now().getMillis();
    final String query = String.format(TEMPLATE, lookupDb, lookupTable, uid);

    @Nullable UserState userState = null;

    try (final Connection conn = this.dataSource.getConnection()) {
      final ResultSet rs = conn.prepareStatement(query).executeQuery();
      if (rs.first()) {
        LOG.info("[@ProcessElement] get from lookup... {}", uid);
        userState = UserState.create(rs.getDouble("total"), rs.getLong("cnt"));
      } else {
        LOG.info("[@ProcessElement] lookup table is empty... {}", uid);
      }
    } catch (SQLException e) {
      LOG.error("[@ProcessElement] lookup table error with {}", uid, e);
    }
    final long delta = Instant.now().getMillis() - sqlBefore;
    this.processingSqlQuery.update(delta);
    return userState;
  }
}
