package com.icloud.fn;

import com.icloud.model.UserState;
import com.icloud.model.UserTransaction;
import java.util.Map;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateUserStateFn extends DoFn<KV<String, UserTransaction>, UserTransaction> {

  private static final Logger LOG = LoggerFactory.getLogger(UpdateUserStateFn.class);

  @StateId("user_state")
  private final StateSpec<@NonNull ValueState<UserState>> userStateSpec = StateSpecs.value();

  @ProcessElement
  public void process(
      @Element KV<String, UserTransaction> element,
      @StateId("user_state") ValueState<UserState> userState,
      @SideInput("storage") Map<String, UserState> storageSideInput,
      OutputReceiver<UserTransaction> output) {

    final UserTransaction userTransaction = element.getValue();
    @NonNull Double newTotal;
    @NonNull Long newCount;
    @Nullable final UserState currentState = userState.read();
    if (currentState != null) {
      LOG.info("currentState is not null... re-calculate total and count");
      newTotal = currentState.getTotal() + userTransaction.getPrice();
      newCount = currentState.getCount() + 1L;
    } else {
      LOG.info("currentState is null... find from storage");
      final UserState stateFromStorage = storageSideInput.get(element.getKey());
      if (stateFromStorage != null) {
        LOG.info("get from storage is not null...");
        newTotal = stateFromStorage.getTotal() + userTransaction.getPrice();
        newCount = stateFromStorage.getCount() + 1;
      } else {
        LOG.info("get from storage is null...");
        newTotal = userTransaction.getPrice();
        newCount = 1L;
      }
    }

    final UserState newState = UserState.create(newTotal, newCount);
    userState.write(newState);

    LOG.info("user state saved...");
    output.output(element.getValue());
  }
}
