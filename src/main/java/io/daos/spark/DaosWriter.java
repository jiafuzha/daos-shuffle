/*
 * (C) Copyright 2018-2020 Intel Corporation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * GOVERNMENT LICENSE RIGHTS-OPEN SOURCE SOFTWARE
 * The Government's rights to use, modify, reproduce, release, perform, display,
 * or disclose this software are subject to the terms of the Apache License as
 * provided in Contract No. B609815.
 * Any reproduction of computer software, computer software documentation, or
 * portions thereof marked with this legend must also reproduce the markings.
 */

package io.daos.spark;

import io.daos.BufferAllocator;
import io.daos.obj.DaosObjClient;
import io.netty.buffershade5.ByteBuf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A DAOS writer per map task
 */
public class DaosWriter {

  private long mapId;

  private int bufferSize;

  private int writeValve;

  private DaosObjClient objClient;

  private Map<Integer, NativeBuffer> partitionBufMap = new HashMap<>();

  public DaosWriter(long mapId, int bufferSize, int writeValve, DaosObjClient objClient) {
    this.mapId = mapId;
    this.bufferSize = bufferSize;
    this.writeValve = writeValve;
    this.objClient = objClient;
  }

  private NativeBuffer getNativeBuffer(int partitionId) {
    NativeBuffer buffer = partitionBufMap.get(partitionId);
    if (buffer == null) {
      buffer = new NativeBuffer(partitionId, bufferSize);
      partitionBufMap.put(partitionId, buffer);
    }
    return buffer;
  }

  public void write(int partitionId, byte b) throws IOException {
    getNativeBuffer(partitionId).write(b);
  }

  public void write(int partitionId, byte[] b) throws IOException {
    getNativeBuffer(partitionId).write(b);
  }

  public void write(int partitionId, byte[] b, int offset, int len) throws IOException {
    getNativeBuffer(partitionId).write(b, offset, len);
  }

  public void flush(int partitionId) throws IOException {
    // TODO: actual write
  }

  public void close() throws IOException {
    // TODO: write rest of partitions
  }

  private class NativeBuffer {
    private int partitionId;
    private int idx;
    private List<ByteBuf> bufList = new ArrayList<>();

    public NativeBuffer(int partitionId, int bufferSize) {
      this.partitionId = partitionId;
      bufList.add(BufferAllocator.objBufWithNativeOrder(bufferSize));
    }

    public void write(byte b) throws IOException {
      if ()
    }

  }
}
