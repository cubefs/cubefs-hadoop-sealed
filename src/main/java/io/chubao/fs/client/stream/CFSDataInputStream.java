// Copyright 2020 The Chubao Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
package io.chubao.fs.client.stream;

import io.chubao.fs.sdk.stream.CFSInputStream;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.ByteBufferPool;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.EnumSet;

public class CFSDataInputStream extends FSInputStream implements ByteBufferReadable,
    HasEnhancedByteBufferAccess, CanUnbuffer {
  private static final Log log = LogFactory.getLog(CFSDataInputStream.class);

  private CFSInputStream input;

  public CFSDataInputStream(CFSInputStream stream) {
    this.input = stream;
  }

  @Override
  public void seek(long pos) throws IOException {
    input.seek(pos);
  }

  @Override
  public long getPos() throws IOException {
    return input.getPos();
  }

  @Override
  public boolean seekToNewSource(long targetPos) throws IOException {
    return input.seekToNewSource(targetPos);
  }

  @Override
  public int read() throws IOException {
    return input.read();
  }

  @Override
  public synchronized int read(final byte buf[], int off, int len) throws IOException {
    if (buf == null) {
      throw new NullPointerException();
    } else if (off < 0 || len < 0 || len > buf.length - off) {
      throw new IndexOutOfBoundsException();
    } else if (len == 0) {
      return 0;
    }

    int size = input.read(buf, off, len);
    return size;
  }

  @Override
  public int read(ByteBuffer byteBuffer) throws IOException {
    return input.read(byteBuffer);
  }

  @Override
  public ByteBuffer read(ByteBufferPool byteBufferPool, int i, EnumSet<ReadOption> enumSet) throws IOException, UnsupportedOperationException {
    throw new IOException("Not implement the read function.");
  }

  @Override
  public void unbuffer() {
    log.error("Not implement unbuffer function.");
  }

  @Override
  public void releaseBuffer(ByteBuffer byteBuffer) {
    log.error("Not implement releaseBuff function.");
  }

  @Override
  public void close() throws IOException {
    input.close();
  }
}
