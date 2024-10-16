package com.icloud.io;

import java.util.*;
import org.apache.beam.sdk.transforms.splittabledofn.HasDefaultTracker;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult;
import org.joda.time.Instant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DirectoryWatchRestriction
    implements HasDefaultTracker<
        DirectoryWatchRestriction, RestrictionTracker<DirectoryWatchRestriction, String>> {

  private final Logger LOG = LoggerFactory.getLogger(DirectoryWatchRestriction.class);

  private final Set<String> alreadyProcessed;

  private boolean finished;

  DirectoryWatchRestriction(Set<String> alreadyProcessed) {
    this.alreadyProcessed = alreadyProcessed;
    this.finished = false;
  }

  public DirectoryWatchRestriction(Set<String> alreadyProcessed, boolean finished) {
    this.alreadyProcessed = alreadyProcessed;
    this.finished = finished;
  }

  public Set<String> getAlreadyProcessed() {
    return alreadyProcessed;
  }

  public boolean isFinished() {
    return finished;
  }

  public DirectoryWatchRestriction asPrimary() {
    this.finished = true;
    return this;
  }

  public DirectoryWatchRestriction asResidual() {
    return new DirectoryWatchRestriction(this.alreadyProcessed, false);
  }

  @Override
  public RestrictionTracker<DirectoryWatchRestriction, String> newTracker() {
    return new RestrictionTracker<>() {

      private final Logger LOG = LoggerFactory.getLogger("DirectoryWatchRestrictionTracker");

      private final DirectoryWatchRestriction restriction = DirectoryWatchRestriction.this;

      @Override
      public boolean tryClaim(String newFile) {
        LOG.info("[@TryClaim] tryClaim called at {}", Instant.now());
        if (this.restriction.isFinished()) {
          LOG.info("[@TryClaim] :::finished:::");
          return false;
        }

        this.restriction.getAlreadyProcessed().add(newFile);
        return true;
      }

      @Override
      public DirectoryWatchRestriction currentRestriction() {
        return this.restriction;
      }

      @Override
      public SplitResult<DirectoryWatchRestriction> trySplit(double fractionOfRemainder) {
        LOG.info("[@TrySplit] called at {}", Instant.now());

        return SplitResult.of(this.restriction.asPrimary(), this.restriction.asResidual());
      }

      @Override
      public void checkDone() throws IllegalStateException {
        LOG.info("[@CheckDone] called at {}", Instant.now());
      }

      @Override
      public IsBounded isBounded() {
        return this.restriction.isFinished() ? IsBounded.BOUNDED : IsBounded.UNBOUNDED;
      }
    };
  }
}
