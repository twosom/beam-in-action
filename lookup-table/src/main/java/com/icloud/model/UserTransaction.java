package com.icloud.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.auto.value.AutoValue;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;

@AutoValue
@DefaultSchema(AutoValueSchema.class)
public abstract class UserTransaction {
  public abstract String getUid();

  public abstract String getItem();

  public abstract Double getPrice();

  @JsonProperty("transaction_id")
  public abstract String getTransactionId();

  @JsonProperty("transaction_ts")
  public abstract Long getTransactionTs();

  public static UserTransaction create(
      String uid, String item, Double price, String transactionId, Long transactionTs) {
    return new AutoValue_UserTransaction.Builder()
        .setUid(uid)
        .setItem(item)
        .setPrice(price)
        .setTransactionId(transactionId)
        .setTransactionTs(transactionTs)
        .build();
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setUid(String uid);

    abstract Builder setItem(String item);

    abstract Builder setPrice(Double price);

    abstract Builder setTransactionId(String transactionId);

    abstract Builder setTransactionTs(Long transactionTs);

    abstract UserTransaction build();
  }
}
