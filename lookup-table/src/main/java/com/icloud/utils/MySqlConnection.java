package com.icloud.utils;

import javax.sql.DataSource;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

public class MySqlConnection {

  @MonotonicNonNull private static volatile MySqlConnection instance = null;
  transient DataSource dataSource;
  private static final Object lock = new Object();

  private MySqlConnection(
      String cloudSqlDb,
      String cloudSqlInstanceConnectionName,
      String cloudSqlUsername,
      String cloudSqlPassword) {
    this.dataSource =
        getDatasourceConfig(
                cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername, cloudSqlPassword)
            .buildDatasource();
  }

  public static JdbcIO.DataSourceConfiguration getDatasourceConfig(
      String cloudSqlDb,
      String cloudSqlInstanceConnectionName,
      String cloudSqlUsername,
      String cloudSqlPassword) {
    return JdbcIO.DataSourceConfiguration.create(
            com.mysql.cj.jdbc.Driver.class.getName(),
            String.format(
                "jdbc:mysql:///%s?"
                    + "cloudSqlInstance=%s&"
                    + "socketFactory=com.google.cloud.sql.mysql.SocketFactory&"
                    + "useUnicode=true&"
                    + "characterEncoding=UTF-8&"
                    + "connectionCollation=utf8mb4_unicode_ci",
                cloudSqlDb, cloudSqlInstanceConnectionName))
        .withUsername(cloudSqlUsername)
        .withPassword(cloudSqlPassword);
  }

  public static MySqlConnection getInstance(
      String cloudSqlDb,
      String cloudSqlInstanceConnectionName,
      String cloudSqlUsername,
      String cloudSqlPassword) {
    if (instance == null) {
      synchronized (lock) {
        if (instance == null) {
          instance =
              new MySqlConnection(
                  cloudSqlDb, cloudSqlInstanceConnectionName, cloudSqlUsername, cloudSqlPassword);
        }
      }
    }

    return instance;
  }

  public DataSource getDataSource() {
    return dataSource;
  }
}
