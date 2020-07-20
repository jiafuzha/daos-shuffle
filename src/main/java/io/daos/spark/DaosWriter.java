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
import io.daos.obj.DaosObject;
import io.daos.obj.IODataDesc;
import io.netty.buffershade5.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * A DAOS writer per map task
 */
public class DaosWriter {

  private long appId;

  private int shuffleId;

  private String mapId;

  private int bufferSize;

  private int minSize;

  private DaosObject object;

  private Map<Integer, NativeBuffer> partitionBufMap = new HashMap<>();

  private static Logger LOG = LoggerFactory.getLogger(DaosWriter.class);

  public DaosWriter(long appId, int shuffleId, long mapId, int bufferSize, int minSize, DaosObject object) {
    this.appId = appId;
    this.shuffleId = shuffleId;
    this.mapId = String.valueOf(mapId);
    this.bufferSize = bufferSize;
    this.minSize = minSize;
    this.object = object;
  }

  private NativeBuffer getNativeBuffer(int partitionId) {
    NativeBuffer buffer = partitionBufMap.get(partitionId);
    if (buffer == null) {
      buffer = new NativeBuffer(partitionId, bufferSize);
      partitionBufMap.put(partitionId, buffer);
    }
    return buffer;
  }

  public void write(int partitionId, int b) {
    getNativeBuffer(partitionId).write(b);
  }

  public void write(int partitionId, byte[] b) {
    getNativeBuffer(partitionId).write(b);
  }

  public void write(int partitionId, byte[] b, int offset, int len) {
    getNativeBuffer(partitionId).write(b, offset, len);
  }

  public long[] getPartitionLens(int numPartitions) {
    long[] lens = new long[numPartitions];
    partitionBufMap.values().stream().sorted().forEach(b -> lens[b.partitionId] = b.totalSize + b.roundSize);
    return lens;
  }

  public void flush(int partitionId) throws IOException {
    NativeBuffer buffer = partitionBufMap.get(partitionId);
    IODataDesc desc = buffer.createUpdateDesc();
    if (desc != null) {
      if (buffer.totalSize < minSize) {
        LOG.warn("too small partition size {}, shuffle {}, map {}, partition {}, app {}",
            buffer.totalSize, shuffleId, mapId, partitionId, appId);
      }
      try {
        object.update(desc);
      }catch (IOException e) {
        throw new IOException("failed to flush rest of partition " + partitionId, e);
      } finally {
        desc.release();
        buffer.reset();
      }
    }
  }

  public void close() throws IOException {
    partitionBufMap.clear();
    partitionBufMap = null;
    object.close();
    object = null;
  }

  /**
   * Write data to one or multiple netty direct buffers which will be written to DAOS without copy
   */
  private class NativeBuffer implements Comparable<NativeBuffer> {
    private int partitionId;
    private String partitionIdKey;
    private int bufferSize;
    private int idx = -1;
    private List<ByteBuf> bufList = new ArrayList<>();
    private long totalSize;
    private long roundSize;

    public NativeBuffer(int partitionId, int bufferSize) {
      this.partitionId = partitionId;
      this.partitionIdKey = String.valueOf(partitionId);
      this.bufferSize = bufferSize;
    }

    private ByteBuf addNewByteBuf(int len) {
      ByteBuf buf = BufferAllocator.objBufWithNativeOrder(Math.max(bufferSize, len));
      bufList.add(buf);
      idx++;
      return buf;
    }

    private ByteBuf getBuffer(int len) {
      if (idx < 0) {
        return addNewByteBuf(len);
      }
      return bufList.get(idx);
    }

    public void write(int b) {
      ByteBuf buf = getBuffer(1);
      if (buf.writableBytes() >= 1) {
        buf.writeByte(b);
      } else {
        buf = addNewByteBuf(1);
        buf.writeByte(b);
      }
      roundSize += 1;
    }

    public void write(byte[] b) {
      write(b, 0, b.length);
    }

    public void write(byte[] b, int offset, int len) {
      ByteBuf buf = getBuffer(len);
      int avail = buf.writableBytes();
      int gap = len - avail;
      if (gap <= 0) {
        buf.writeBytes(b, offset, len);
      } else {
        buf.writeBytes(b, offset, avail);
        buf = addNewByteBuf(gap);
        buf.writeBytes(b, avail, gap);
      }
      roundSize += len;
    }

    public IODataDesc createUpdateDesc() throws IOException {
      if (roundSize == 0) {
        return null;
      }
      List<IODataDesc.Entry> entries = new ArrayList<>();
      bufList.forEach(buf -> {
        try {
          entries.add(
              IODataDesc.createEntryForUpdate(
              mapId,
              IODataDesc.IodType.ARRAY,
              1,
                  (int)totalSize,
              buf
              ));
        } catch (IOException e) {
          throw new RuntimeException("failed to create update entry", e);
        }
      });
      return object.createDataDescForUpdate(partitionIdKey, entries);
    }

    public void reset() {
      bufList.forEach(b -> b.release());
      bufList.clear();
      idx = -1;
      totalSize += roundSize;
      roundSize = 0;
    }

    @Override
    public int compareTo(NativeBuffer nativeBuffer) {
      return partitionId - nativeBuffer.partitionId;
    }
  }
}
