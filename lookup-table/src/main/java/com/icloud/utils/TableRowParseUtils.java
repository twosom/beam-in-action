package com.icloud.utils;

import com.google.api.services.bigquery.model.TableRow;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

public class TableRowParseUtils {
  private TableRowParseUtils() {}

  public static int getIntValue(final TableRow row, final String columnName) {
    final Object value = row.get(columnName);
    if (value == null) {
      return 0;
    }
    return Integer.parseInt(value.toString());
  }

  public static long getLongValue(final TableRow row, final String columnName) {
    final Object value = row.get(columnName);
    if (value == null) {
      return 0L;
    }
    return Long.parseLong(value.toString());
  }

  public static Double getDoubleValue(final TableRow row, String column) {
    Object value = row.get(column);
    if (value == null) {
      return 0.0;
    }
    return Double.parseDouble(value.toString());
  }

  public static @NonNull String getStringValue(final TableRow row, String column) throws Exception {
    return getStringValue(row, column, false);
  }

  public static @Nullable String getStringValue(
      final TableRow row, String column, boolean isNullable) throws Exception {
    Object value = row.get(column);
    if (value == null) {
      if (!isNullable) {
        throw new Exception("Not nullable value is NULL");
      }
      return null;
    }
    return value.toString();
  }
}
