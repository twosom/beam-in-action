package com.icloud;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollectionView;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SdfExample {

  private static final String TEXT =
      "Lorem Ipsum is simply dummy text of the printing and typesetting industry. "
          + "Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, "
          + "when an unknown printer took a galley of type and scrambled it to make a type specimen book. "
          + "It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. "
          + "It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, "
          + "and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.";

  public static void main(String[] args) throws InterruptedException {
    final Pipeline pipeline = PipelineUtils.create(args);

    final PCollectionView<String> view =
        pipeline.apply("Create Foo", Create.of("foo")).apply("Foo Side Input", View.asSingleton());

    pipeline
        .apply("Read Text", Create.of(TEXT))
        .apply("Splittable Do Fn", ParDo.of(new SplitDoFn()).withSideInput("sideInput", view))
        .apply("Do Logging", LogUtils.of());

    pipeline.run();

    Thread.sleep(Long.MAX_VALUE);
  }

  private static final class SplitDoFn extends DoFn<String, KV<Long, String>> {

    /** for sdf batch size */
    private static final long batchSize = 5;

    private static final Logger LOG = LoggerFactory.getLogger(SplitDoFn.class);

    @GetInitialRestriction
    public HopeOffsetRange getInitialRestriction(@Element String element) {
      return HopeOffsetRange.of(0, element.split(" ").length);
    }

    @GetRestrictionCoder
    public Coder<HopeOffsetRange> getRestrictionCoder() throws NoSuchSchemaException {
      return SchemaRegistry.createDefault().getSchemaCoder(HopeOffsetRange.class);
    }

    @SplitRestriction
    public void splitRestriction(
        @Restriction HopeOffsetRange restriction, OutputReceiver<HopeOffsetRange> output) {
      LOG.info("[@SplitRestriction] called at {}", Instant.now());
      long from = restriction.getFrom();
      final long outputSize = restriction.getTo() / batchSize;
      while (from < restriction.getTo() - outputSize) {
        long end = from + outputSize;
        HopeOffsetRange split = HopeOffsetRange.of(from, end);
        output.output(split);
        LOG.info("[@SplitRestriction] emit split restriction with value {}", split);
        from = end;
      }

      var split = HopeOffsetRange.of(from, restriction.getTo());
      LOG.info("[@SplitRestriction] emit split restriction with value {}", split);
      output.output(split);
    }

    @ProcessElement
    public void process(
        @Element String element,
        @SideInput("sideInput") String sideInput,
        RestrictionTracker<HopeOffsetRange, Long> tracker,
        OutputReceiver<KV<Long, String>> output) {
      final HopeOffsetRange restriction = tracker.currentRestriction();
      System.out.println(sideInput);
      assert restriction != null : "restriction can not be null";
      for (var i = restriction.getFrom(); tracker.tryClaim(i); i++) {
        output.output(KV.of(i, element.split(" ")[(int) i]));
      }
    }
  }
}
