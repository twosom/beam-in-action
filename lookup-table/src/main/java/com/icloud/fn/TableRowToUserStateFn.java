package com.icloud.fn;

import com.google.api.services.bigquery.model.TableRow;
import com.icloud.model.UserState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class TableRowToUserStateFn extends DoFn<TableRow, KV<String, UserState>> {
  @ProcessElement
  public void process(@Element TableRow element, OutputReceiver<KV<String, UserState>> output) {
    final String uid = (String) element.get("uid");
    final Double total = (Double) element.get("total");
    final long count = Long.parseLong(element.get("n").toString());
    output.output(KV.of(uid, UserState.create(total, count)));
  }
}
