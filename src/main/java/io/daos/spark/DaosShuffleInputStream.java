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

import io.daos.obj.DaosObject;
import io.daos.obj.IODataDesc;
import io.netty.buffershade5.ByteBuf;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.shuffle.daos.package$;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple3;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

@NotThreadSafe
public class DaosShuffleInputStream extends InputStream {

  private DaosReader reader;

  private DaosObject object;

  private Executor executor;

  private ReaderConfig config;

  private ShuffleReadMetricsReporter metrics;

  private boolean fromOtherThread = true;

  private boolean cleaned;

  private LinkedHashMap<Integer, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap; // ensure the order of partition
  private Iterator<Integer> mapIdIt;

  private BufferSource source = new BufferSource();

  private static final Logger log = LoggerFactory.getLogger(DaosShuffleInputStream.class);

  public DaosShuffleInputStream(DaosReader reader, LinkedHashMap<Integer, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap,
                                long maxBytesInFlight, long maxMem, ShuffleReadMetricsReporter metrics) {
    this.reader = reader;
    reader.register(source);
    this.executor = reader.nextReaderExecutor();
    this.object = reader.getObject();
    this.config = new ReaderConfig(maxBytesInFlight, maxMem);
    this.partSizeMap = partSizeMap;
    this.metrics = metrics;
    this.mapIdIt = partSizeMap.keySet().iterator();
  }

  public BlockId getCurBlockId() {
    if (source.curMapId < 0) {
      return null;
    }
    return partSizeMap.get(source.curMapId)._2();
  }

  public BlockManagerId getCurOriginAddress() {
    if (source.curMapId < 0) {
      return null;
    }
    return partSizeMap.get(source.curMapId)._3();
  }

  public int getCurMapIndex() {
    return source.curMapId;
  }

  @Override
  public int read() throws IOException {
    while (true) {
      ByteBuf buf = source.nextBuf();
      if (buf == null) { // reach end
        return completeAndCleanup();
      }
      if (buf.readableBytes() >= 1) {
        metrics.incRemoteBytesRead(1);
        return buf.readByte();
      }
    }
  }

  @Override
  public int read(byte[] bytes) throws IOException {
    int len = read(bytes, 0, bytes.length);
    metrics.incRemoteBytesRead(len);
    return len;
  }

  @Override
  public int read(byte[] bytes, int offset, int length) throws IOException {
    int len = length;
    while (true) {
      ByteBuf buf = source.nextBuf();
      if (buf == null) { // reach end
        return completeAndCleanup();
      }
      if (len <= buf.readableBytes()) {
        buf.readBytes(bytes, offset, len);
        metrics.incRemoteBytesRead(length);
        return length;
      }
      int maxRead = buf.readableBytes();
      buf.readBytes(bytes, offset, maxRead);
      offset += maxRead;
      len -= maxRead;
    }
  }

  private int completeAndCleanup() throws IOException {
    source.checkPartitionSize(source.currentEntry.getKey());
    return -1;
  }

  private void cleanup() {
    if (!cleaned) {
      boolean allReleased = source.cleanup(false);
      if (allReleased) {
        reader.unregister(source);
      }
      source = null;
      cleaned = true;
    }
  }

  @Override
  public void close() {
    cleanup();
  }

  public class BufferSource {
    private DaosReader.ReadTaskContext currentCtx;
    private DaosReader.ReadTaskContext lastCtx;
    private Deque<DaosReader.ReadTaskContext> consumedStack = new LinkedList<>();
    private Set<DaosReader.ReadTaskContext> discardedSet = new HashSet<>();
    private IODataDesc currentDesc;
    private IODataDesc.Entry currentEntry;
    private long currentPartSize;

    private int entryIdx;
    private int curMapId = -1;
    private int curOffset;

    private Lock takeLock = new ReentrantLock();
    private Condition notEmpty = takeLock.newCondition();
    private AtomicInteger counter = new AtomicInteger(0);

    private int totalParts = partSizeMap.size();
    private int partsRead;

    private int exceedWaitTimes;
    private long totalInMemSize;
    private int totalSubmitted;

    public ByteBuf readBySelf() throws IOException {
      if (currentCtx != null) {
        return readDuplicated();
      }
      IODataDesc desc = createNextDesc(config.maxBytesInFlight);
      return getBySelf(desc);
    }

    public ByteBuf nextBuf() throws IOException {
      ByteBuf buf = tryCurrentEntry();
      if (buf != null) {
        return buf;
      }
      // next entry
      buf = tryCurrentDesc();
      if (buf != null) {
        return buf;
      }
      // from next partition
      if (fromOtherThread) {
        // next ready queue
        if (currentCtx != null) {
          return tryNextTaskContext();
        }
        // get data by self and submit request for remaining data
        return getBySelfAndSubmitMore(config.minReadSize);
      }
      // get data by self
      return readBySelf();
    }

    private ByteBuf tryNextTaskContext() throws IOException {
      if (partsRead == totalParts) { // all partitions are read
        return null;
      }
      if (partsRead > totalParts) {
        throw new IllegalStateException("partsRead (" + partsRead + ") should not be bigger than totalParts (" +
            totalParts + ")");
      }
      try {
        IODataDesc desc;
        if ((desc = tryGetFromOtherThread()) != null) {
          submitMore();
          return validateAndGetBuf(desc.getEntry(entryIdx));
        }
        // duplicate and get data by self
        // discard current context
        return readDuplicated();
      } catch (InterruptedException e) {
        throw new IOException("read interrupted.", e);
      }
    }

    private ByteBuf readDuplicated() throws IOException {
      discardedSet.add(currentCtx);
      currentCtx = currentCtx.getNext();
      IODataDesc newDesc = currentDesc.duplicate();
      return getBySelf(newDesc);
    }

    /* put last task context to consumedStack */
    private IODataDesc tryGetFromOtherThread() throws InterruptedException, IOException {
      IODataDesc desc = tryGetValidReady();
      if (desc != null) {
        return desc;
      }
      // wait for specified time
      desc = waitForValidFromOtherThread();
      if (desc != null) {
        return desc;
      }
      // check wait times and cancel task
      boolean cancelAll = false;
      if (exceedWaitTimes >= config.waitTimoutTimes) {
        fromOtherThread = false;
        log.info("stop reading from dedicated read thread");
        cancelAll = true;
      } else {
        // try again
        desc = tryGetValidReady();
        if (desc != null) {
          return desc;
        }
      }
      // cancel tasks
      cancelTasks(cancelAll);
      return null;
    }

    private IODataDesc waitForValidFromOtherThread() throws InterruptedException, IOException {
      IODataDesc desc;
      while (true) {
        boolean timeout = false;
        takeLock.lockInterruptibly();
        try {
          long start = System.nanoTime();
          if (!notEmpty.await(config.waitDataTimeMs, TimeUnit.MILLISECONDS)) {
            timeout = true;
          }
          metrics.incFetchWaitTime(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start));
        } finally {
          takeLock.unlock();
        }
        if (timeout) {
          log.warn("exceed wait: {}ms, times: {}", config.waitDataTimeMs, exceedWaitTimes);
          exceedWaitTimes++;
          return null;
        }
        // get some results after wait
        int c = counter.getAndDecrement();
        if (c < 0) {
          throw new IllegalStateException("counter should be non-negative, " + c);
        }
        desc = tryGetValidReady();
        if (desc != null) {
          return desc;
        }
      }
    }

    private void cancelTasks(boolean cancelAll) {
      currentCtx.cancel();
      if (cancelAll) {
        DaosReader.ReadTaskContext ctx = currentCtx.getNext();
        while (ctx != null) {
          ctx.cancel();
          ctx = ctx.getNext();
        }
      }
    }

    private IODataDesc tryGetValidReady() throws IOException {
      while (counter.get() > 0) {
        counter.getAndDecrement();
        IODataDesc desc = moveToNextDesc();
        if (desc != null) {
          return desc;
        }
      }
      return null;
    }

    private IODataDesc moveToNextDesc() throws IOException {
      consumedStack.push(currentCtx);
      totalInMemSize -= currentCtx.getDesc().getTotalRequestSize();
      totalSubmitted -= 1;
      currentCtx = currentCtx.getNext();
      IODataDesc desc = validateAndSetCurDesc(currentCtx, true);
      if (desc == null) {
        return null;
      }
      metrics.incRemoteBlocksFetched(1);
      currentDesc = currentCtx.getDesc();
      return currentDesc;
    }

    private ByteBuf tryCurrentDesc() throws IOException {
      if (currentDesc != null) {
        ByteBuf buf;
        while (entryIdx < currentDesc.numberOfEntries()) {
          IODataDesc.Entry entry = currentDesc.getEntry(entryIdx);
          buf = validateAndGetBuf(entry);
          if (buf.readableBytes() > 0) {
            return buf;
          }
          entryIdx++;
        }
        entryIdx = 0;
        // no need to release desc since all its entries are released in tryCurrentEntry and
        // internal buffers are released after object.fetch
        // reader.close will release all in case of failure
        currentDesc = null;
      }
      return null;
    }

    private ByteBuf tryCurrentEntry() {
      if (currentEntry != null) {
        ByteBuf buf = currentEntry.getFetchedData();
        if (buf.readableBytes() > 0) {
          return buf;
        }
        // release buffer as soon as possible
        currentEntry.releaseFetchDataBuffer();
        entryIdx++;
      }
      // not null currentEntry since it will be used for size validation
      return null;
    }

    /**
     * for first read.
     *
     * @param selfReadLimit
     * @return
     * @throws IOException
     */
    private ByteBuf getBySelfAndSubmitMore(long selfReadLimit) throws IOException {
      entryIdx = 0;
      // fetch the next by self
      IODataDesc desc = createNextDesc(selfReadLimit);
      try {
        if (fromOtherThread) {
          submitMore();
        }
      } catch (Exception e) {
        desc.release();
        if (e instanceof IOException) {
          throw (IOException)e;
        }
        throw new IOException("failed to submit more", e);
      }
      return getBySelf(desc);
    }

    private void submitMore() throws IOException {
      while (totalSubmitted < config.readBatchSize && totalInMemSize < config.maxMem) {
        IODataDesc taskDesc = createNextDesc(config.maxBytesInFlight);
        if (taskDesc == null) {
          break;
        }
        totalInMemSize += taskDesc.getTotalRequestSize();
        totalSubmitted++;
        DaosReader.ReadTaskContext context = tryReuseContext(taskDesc);
        executor.execute(DaosReader.ReadTask.newInstance(context));
      }
    }

    private DaosReader.ReadTaskContext tryReuseContext(IODataDesc desc) {
      DaosReader.ReadTaskContext context = consumedStack.pop();
      if (context != null) {
        context.reuse(desc);
      } else {
        context = new DaosReader.ReadTaskContext(object, counter, takeLock, notEmpty, desc);
      }
      lastCtx.setNext(context);
      lastCtx = context;
      return context;
    }

    private ByteBuf getBySelf(IODataDesc desc) throws IOException {
      // get data by self, no need to release currentDesc
      if (desc == null) { // reach end
        return null;
      }
      boolean releaseBuf = false;
      try {
        object.fetch(desc);
        metrics.incRemoteBlocksFetched(1);
        currentDesc = desc;
        return validateAndGetBuf(desc.getEntry(entryIdx));
      } catch (IOException | IllegalStateException e) {
        releaseBuf = true;
        throw e;
      } finally {
        desc.release(releaseBuf);
      }
    }

    private IODataDesc createNextDesc(long sizeLimit) throws IOException {
      List<IODataDesc.Entry> entries = new ArrayList<>();
      long remaining = sizeLimit;
      while (remaining > 0) {
        int mapId = getMapId();
        if (mapId < 0) {
          break;
        }
        long readSize = partSizeMap.get(mapId)._1() - curOffset;
        long offset = curOffset;
        if (readSize > remaining) {
          readSize = remaining;
          curMapId = mapId;
          curOffset += readSize;
        } else {
          curMapId = -1;
        }
        entries.add(createEntry(mapId, offset, readSize));
        remaining -= readSize;
      }
      if (entries.isEmpty()) {
        return null;
      }
      return object.createDataDescForFetch(String.valueOf(reader.getReduceId()), entries);
    }

    private int getMapId() {
      int mapId = curMapId;
      if (mapId >= 0) {
        return mapId;
      }
      curOffset = 0;
      if (mapIdIt.hasNext()) {
        mapId = mapIdIt.next();
        partsRead++;
        return mapId;
      }
      return -1;
    }

    private IODataDesc.Entry createEntry(int mapId, long offset, long readSize) throws IOException {
      return IODataDesc.createEntryForFetch(String.valueOf(mapId), IODataDesc.IodType.ARRAY, 1, (int)offset,
          (int)readSize);
    }

    private IODataDesc validateAndSetCurDesc(DaosReader.ReadTaskContext context, boolean checkDiscarded)
        throws IOException {
      if (checkDiscarded && discardedSet.contains(context)) {
        context.getDesc().release();
        discardedSet.remove(context);
        consumedStack.push(context);
        return null;
      }
      IODataDesc desc = context.getDesc();
      if (!desc.succeeded()) {
        String msg = "failed to get data from DAOS, desc: " + desc.toString(4096);
        if (desc.getCause() != null) {
          throw new IOException(msg, desc.getCause());
        } else {
          throw new IllegalStateException(msg + "\nno exception got. logic error or crash?");
        }
      }
      currentDesc = desc;
      return desc;
    }

    private ByteBuf validateAndGetBuf(IODataDesc.Entry entry) throws IOException {
      String akey = entry.getKey();
      ByteBuf buf = entry.getFetchedData();
      if (currentEntry != null && !akey.equals(currentEntry.getKey())) {
        checkPartitionSize(currentEntry.getKey());
        currentPartSize = 0L;
        currentPartSize += buf.readableBytes();
      }
      currentEntry = entry;
      return buf;
    }

    private void checkPartitionSize(String curAkey) throws IOException {
      if (currentPartSize != partSizeMap.get(Integer.valueOf(curAkey))._1()) {
        throw new IOException("expect partition size " + partSizeMap.get(Integer.valueOf(curAkey)) +
            ", actual size " + currentPartSize);
      }
    }

    public boolean cleanup(boolean force) {
      boolean allReleased = true;
      if (!cleaned) {
        allReleased &= cleanupTaskContext(currentCtx, force);
        allReleased &= cleanupTaskContexts(consumedStack, force);
        allReleased &= cleanupTaskContexts(discardedSet, force);
      }
      return allReleased;
    }

    private boolean cleanupTaskContexts(Collection<DaosReader.ReadTaskContext> collection, boolean force) {
      boolean allReleased = true;
      for (DaosReader.ReadTaskContext ctx : collection) {
        allReleased &= cleanupTaskContext(ctx, force);
      }
      if (allReleased) {
        collection.clear();
      }
      return allReleased;
    }

    private boolean cleanupTaskContexts(DaosReader.ReadTaskContext head, boolean force) {
      boolean allReleased = true;
      while(head != null) {
        allReleased &= cleanupTaskContext(head, force);
        head = head.getNext();
      }
      return allReleased;
    }

    private boolean cleanupTaskContext(DaosReader.ReadTaskContext ctx, boolean force) {
      IODataDesc desc = ctx.getDesc();
      if (desc != null && (force || desc.succeeded() || ctx.isCancelled())) {
        desc.release();
        return true;
      }
      return false;
    }
  }

  private static final class ReaderConfig {
    private long minReadSize;
    private long maxBytesInFlight;
    private long maxMem;
    private int readBatchSize;
    private int waitDataTimeMs;
    private int waitTimoutTimes;

    private ReaderConfig(long maxBytesInFlight, long maxMem) {
      SparkConf conf = SparkEnv.get().conf();
      minReadSize = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_MINIMUM_SIZE()) * 1024 * 1024;
      if (maxBytesInFlight < minReadSize) {
        this.maxBytesInFlight = minReadSize;
      } else {
        this.maxBytesInFlight = maxBytesInFlight;
      }
      this.maxMem = maxMem;
      this.readBatchSize = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_BATCH_SIZE());
      this.waitDataTimeMs = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_MS());
      this.waitTimoutTimes = (int)conf.get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_TIMEOUT_TIMES());
    }

  }
}
