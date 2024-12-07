package com.icloud;

import static com.google.common.base.MoreObjects.firstNonNull;

import org.apache.beam.sdk.io.GenerateSequence;
import org.apache.beam.sdk.state.StateSpec;
import org.apache.beam.sdk.state.StateSpecs;
import org.apache.beam.sdk.state.TimeDomain;
import org.apache.beam.sdk.state.Timer;
import org.apache.beam.sdk.state.TimerSpec;
import org.apache.beam.sdk.state.TimerSpecs;
import org.apache.beam.sdk.state.ValueState;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.WithKeys;
import org.apache.beam.sdk.values.KV;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoopingTimerPipeline {
  public static void main(String[] args) {
    var p = PipelineUtils.create(args);

    p.apply(GenerateSequence.from(0).withRate(1, Duration.standardSeconds(10)))
        .apply(WithKeys.of("some-key"))
        .apply(ParDo.of(new LoopingTimerFn()))
        .apply(LogUtils.of());

    p.run();
  }

  private static class LoopingTimerFn extends DoFn<KV<String, Long>, String> {

    private final Logger LOG = LoggerFactory.getLogger(LoopingTimerFn.class);

    @StateId("timer-set")
    private final StateSpec<@NonNull ValueState<Boolean>> timerSetStateSpec = StateSpecs.value();

    @TimerId("some-timer")
    private final TimerSpec someTimerSpec = TimerSpecs.timer(TimeDomain.EVENT_TIME);

    @ProcessElement
    public void process(
        @Element KV<String, Long> element,
        @TimerId("some-timer") Timer someTimer,
        @StateId("timer-set") ValueState<Boolean> timerSetState) {
      final boolean timerSet = firstNonNull(timerSetState.read(), false);
      LOG.info("[@ProcessElement] {}", element);
      if (!timerSet) {
        someTimer.offset(Duration.millis(50)).setRelative();
        timerSetState.write(true);
      }
    }

    @OnTimer("some-timer")
    public void onTimer(@TimerId("some-timer") Timer someTimer, OnTimerContext context) {
      someTimer.offset(Duration.standardSeconds(1L)).setRelative();
      context.output(".");
    }
  }
}
