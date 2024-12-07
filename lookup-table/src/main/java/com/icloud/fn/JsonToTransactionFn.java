package com.icloud.fn;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.icloud.model.UserTransaction;
import java.util.Map;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JsonToTransactionFn extends DoFn<String, KV<String, UserTransaction>> {

  private static final Logger LOG = LoggerFactory.getLogger(JsonToTransactionFn.class);
  private transient ObjectMapper mapper;
  private final Counter jsonParsingErrorCounter =
      Metrics.counter(JsonToTransactionFn.class, "json-parsing-error-count");

  @Setup
  public void setup() {
    this.mapper = new ObjectMapper();
  }

  @ProcessElement
  public void process(@Element String element, OutputReceiver<KV<String, UserTransaction>> output) {
    try {
      final Map<String, Object> jsonMap = mapper.readValue(element, new TypeReference<>() {});

      final String uid = (String) jsonMap.get("uid");
      final String item = (String) jsonMap.get("item");
      final Double price = Double.valueOf(jsonMap.get("price").toString());
      final String transactionId = (String) jsonMap.get("transaction_id");
      final Long transactionTs = (Long) jsonMap.get("transaction_ts");
      final UserTransaction userTransaction =
          UserTransaction.create(uid, item, price, transactionId, transactionTs);
      output.output(KV.of(userTransaction.getUid(), userTransaction));

    } catch (JsonProcessingException e) {
      LOG.warn("[JsonProcessingException] with value {}", element);
      this.jsonParsingErrorCounter.inc();
    }
  }

  @Teardown
  public void teardown() {
    if (this.mapper != null) {
      this.mapper = null;
    }
  }
}
