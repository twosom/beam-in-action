package com.icloud.model;

import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class UserState {
  public abstract Double getTotal();

  public abstract Long getCount();

  public static UserState create(Double total, Long count) {
    return new AutoValue_UserState.Builder().setTotal(total).setCount(count).build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setTotal(Double total);

    abstract Builder setCount(Long count);

    abstract UserState build();
  }
}
