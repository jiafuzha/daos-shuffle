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

package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObject;
import io.daos.obj.IODataDesc;
import io.netty.util.internal.ObjectPool;
import org.apache.spark.SparkConf;
import org.apache.spark.launcher.SparkLauncher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class DaosReader {

  private DaosObject object;

  private Map<DaosShuffleInputStream.BufferSource, Integer> bufferSourceMap = new ConcurrentHashMap<>();

  private static final SparkConf conf = new SparkConf(false);
  private static boolean fromOtherThread = (boolean)conf
      .get(package$.MODULE$.SHUFFLE_DAOS_READ_FROM_OTHER_THREAD());

  private static int threads = conf.getInt(package$.MODULE$.SHUFFLE_DAOS_READ_THREADS().key(),
      conf.getInt(SparkLauncher.EXECUTOR_CORES, 1));

  private static final BoundThreadExecutors executors;

  private static Logger logger = LoggerFactory.getLogger(DaosReader.class);

  static {
    if (fromOtherThread) {
      executors = new BoundThreadExecutors("read_executors", threads,
          new ReadThreadFactory());
    } else {
      executors = null;
    }
  }

  public DaosReader(DaosObject object) {
    this.object = object;
  }

  public DaosObject getObject() {
    return object;
  }

  public static boolean hasExecutors() {
    return executors != null;
  }

  public BoundThreadExecutors.SingleThreadExecutor nextReaderExecutor() {
    if (executors != null) {
      return executors.nextExecutor();
    }
    return null;
  }

  public void close() throws IOException {
    object.close();
    // force releasing
    bufferSourceMap.forEach((k, v) -> k.cleanup(true));
    bufferSourceMap.clear();
  }

  public static void stopExecutor() {
    if (executors != null) {
      executors.stop();
    }
  }

  @Override
  public String toString() {
    return "DaosReader{" +
        "object=" + object +
        '}';
  }

  public void register(DaosShuffleInputStream.BufferSource source) {
    bufferSourceMap.put(source, 1);
  }

  public void unregister(DaosShuffleInputStream.BufferSource source) {
    bufferSourceMap.remove(source);
  }

  final static class ReadTask implements Runnable {
    private ReadTaskContext context;
    private final ObjectPool.Handle<ReadTask> handle;

    private final static ObjectPool<ReadTask> objectPool = ObjectPool.newPool(handle -> new ReadTask(handle));

    private static final Logger log = LoggerFactory.getLogger(ReadTask.class);

    static ReadTask newInstance(ReadTaskContext context) {
      ReadTask task = objectPool.get();
      task.context = context;
      return task;
    }

    private ReadTask(ObjectPool.Handle<ReadTask> handle) {
      this.handle = handle;
    }

    @Override
    public void run() {
      boolean cancelled = context.cancelled;
      try {
        if (!cancelled) {
          context.object.fetch(context.desc);
        }
      } catch (Exception e) {
        log.error("failed to read for " + context.desc, e);
      } finally {
        context.counter.getAndIncrement();
        context.signal();
        context.desc.release(cancelled);
        context = null;
        handle.recycle(this);
      }
    }
  }

  /**
   * should be cached in caller thread.
   */
  final static class ReadTaskContext {
    private final DaosObject object;
    private final AtomicInteger counter;
    private final Lock takeLock;
    private final Condition notEmpty;
    private IODataDesc desc;
    private Tuple2<Integer, Integer> mapReduceId;
    private volatile boolean cancelled; // for multi-thread
    private boolean cancelledByCaller; // for accessing by caller
    private ReadTaskContext next;

    public ReadTaskContext(DaosObject object, AtomicInteger counter, Lock takeLock, Condition notEmpty,
                           IODataDesc desc, Tuple2<Integer, Integer> mapReduceId) {
      this.object = object;
      this.counter = counter;
      this.takeLock = takeLock;
      this.notEmpty = notEmpty;
      this.desc = desc;
      this.mapReduceId = mapReduceId;
    }

    public void reuse(IODataDesc desc, Tuple2<Integer, Integer> mapReduceId) {
      this.desc = desc;
      this.mapReduceId = mapReduceId;
      cancelled = false;
      cancelledByCaller = false;
      next = null;
    }

    public void setNext(ReadTaskContext next) {
      this.next = next;
    }

    public ReadTaskContext getNext() {
      return next;
    }

    public IODataDesc getDesc() {
      return desc;
    }

    public void signal() {
      takeLock.lock();
      try {
        notEmpty.signal();
      } finally {
        takeLock.unlock();
      }
    }

    public Tuple2<Integer, Integer> getMapReduceId() {
      return mapReduceId;
    }

    public void cancel() {
      cancelled = true;
      cancelledByCaller = true;
    }

    public boolean isCancelled() {
      return cancelledByCaller;
    }
  }

  private static class ReadThreadFactory implements ThreadFactory {
    private AtomicInteger id = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable runnable) {
      Thread t;
      String name = "daos_read_" + id.getAndIncrement();
      if (runnable == null) {
        t = new Thread(name);
      } else {
        t = new Thread(runnable, name);
      }
      t.setDaemon(true);
      t.setUncaughtExceptionHandler((thread, throwable) ->
          logger.error("exception occurred in thread " + name, throwable));
      return t;
    }
  }

}
