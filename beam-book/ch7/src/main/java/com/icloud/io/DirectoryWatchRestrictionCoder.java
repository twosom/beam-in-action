package com.icloud.io;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import org.apache.beam.sdk.coders.*;

class DirectoryWatchRestrictionCoder extends CustomCoder<DirectoryWatchRestriction> {

  static DirectoryWatchRestrictionCoder of() {
    return new DirectoryWatchRestrictionCoder();
  }

  private DirectoryWatchRestrictionCoder() {}

  private static final SetCoder<String> SET_STRING_CODER = SetCoder.of(StringUtf8Coder.of());
  private static final BooleanCoder BOOL_CODER = BooleanCoder.of();

  @Override
  public void encode(DirectoryWatchRestriction value, OutputStream outStream)
      throws CoderException, IOException {
    SET_STRING_CODER.encode(value.getAlreadyProcessed(), outStream);
    BOOL_CODER.encode(value.isFinished(), outStream);
  }

  @Override
  public DirectoryWatchRestriction decode(InputStream inStream) throws CoderException, IOException {
    final Set<String> alreadyProcessed = SET_STRING_CODER.decode(inStream);
    final boolean finished = BOOL_CODER.decode(inStream);

    return new DirectoryWatchRestriction(alreadyProcessed, finished);
  }
}
