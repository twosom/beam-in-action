package com.icloud;

import java.util.Objects;
import org.apache.avro.reflect.Nullable;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.schemas.annotations.SchemaCreate;

@DefaultCoder(AvroCoder.class)
public class GameActionInfo {
  @Nullable String user;
  @Nullable String team;
  @Nullable Integer score;
  @Nullable Long timestamp;

  public GameActionInfo() {}

  @SchemaCreate
  public GameActionInfo(String user, String team, Integer score, Long timestamp) {
    this.user = user;
    this.team = team;
    this.score = score;
    this.timestamp = timestamp;
  }

  public String getUser() {
    return this.user;
  }

  public String getTeam() {
    return this.team;
  }

  public Integer getScore() {
    return this.score;
  }

  public Long getTimestamp() {
    return this.timestamp;
  }

  public String getKey(String keyname) {
    if ("team".equals(keyname)) {
      return this.team;
    } else { // return username as default
      return this.user;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || o.getClass() != this.getClass()) {
      return false;
    }

    GameActionInfo gameActionInfo = (GameActionInfo) o;

    if (!this.getUser().equals(gameActionInfo.getUser())) {
      return false;
    }

    if (!this.getTeam().equals(gameActionInfo.getTeam())) {
      return false;
    }

    if (!this.getScore().equals(gameActionInfo.getScore())) {
      return false;
    }

    return this.getTimestamp().equals(gameActionInfo.getTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(user, team, score, timestamp);
  }
}
