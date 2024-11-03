package com.icloud;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.fn.splittabledofn.RestrictionTrackers;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker.HasProgress;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.junit.jupiter.api.Test;

class RestrictionTrackerTests {

  @Test
  void testObservingClaims() {
    RestrictionTracker<String, String> observedTracker =
        new RestrictionTracker<>() {
          @Override
          public boolean tryClaim(String position) {
            return "goodClaim".equals(position);
          }

          @Override
          public String currentRestriction() {
            throw new UnsupportedOperationException();
          }

          @Override
          public SplitResult<String> trySplit(double fractionOfRemainder) {
            throw new UnsupportedOperationException();
          }

          @Override
          public void checkDone() throws IllegalStateException {}

          @Override
          public IsBounded isBounded() {
            return IsBounded.BOUNDED;
          }
        };

    final List<String> positionObserved = new ArrayList<>();
    RestrictionTrackers.ClaimObserver<String> observer =
        new RestrictionTrackers.ClaimObserver<>() {
          @Override
          public void onClaimed(String position) {
            System.out.println("onClaimed called...");
            positionObserved.add(position);
            assertEquals("goodClaim", position);
          }

          @Override
          public void onClaimFailed(String position) {
            System.out.println("onClaimFailed called...");
            positionObserved.add(position);
          }
        };

    final RestrictionTracker<String, String> observingTracker =
        RestrictionTrackers.observe(observedTracker, observer);

    observingTracker.tryClaim("goodClaim");
    observingTracker.tryClaim("badClaim");

    assertThat(positionObserved, contains("goodClaim", "badClaim"));
  }

  private static class RestrictionTrackerWithProgress extends RestrictionTracker<Object, Object>
      implements HasProgress {

    @Override
    public Progress getProgress() {
      return RestrictionTracker.Progress.from(2.0, 3.0);
    }

    @Override
    public boolean tryClaim(Object position) {
      return false;
    }

    @Override
    public Object currentRestriction() {
      return null;
    }

    @Override
    public SplitResult<Object> trySplit(double fractionOfRemainder) {
      return null;
    }

    @Override
    public void checkDone() throws IllegalStateException {}

    @Override
    public IsBounded isBounded() {
      return IsBounded.BOUNDED;
    }
  }

  @Test
  void testClaimObserversMaintainBacklogInterfaces() {
    RestrictionTracker<?, ?> hasSize =
        RestrictionTrackers.observe(new RestrictionTrackerWithProgress(), null);
    assertThat(hasSize, instanceOf(HasProgress.class));
  }
}
