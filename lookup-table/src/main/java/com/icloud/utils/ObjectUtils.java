package com.icloud.utils;

import org.checkerframework.checker.nullness.qual.Nullable;

public class ObjectUtils {
  private ObjectUtils() {}

  public static <T> @Nullable T firstNonNull(T first, T second) {
    return first != null ? first : second;
  }
}
