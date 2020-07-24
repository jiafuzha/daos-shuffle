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

import org.apache.spark.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class BoundThreadExecutors {

  private final int threads;

  private final String name;

  private volatile SingleThreadExecutor[] executors;

  private AtomicInteger idx = new AtomicInteger(0);

  private ThreadFactory threadFactory;

  public static final int NOT_STARTED = 0;
  public static final int STARTING = 1;
  public static final int STARTED = 2;
  public static final int STOPPING = 3;
  public static final int STOPPED = 4;

  private static final Logger logger = LoggerFactory.getLogger(BoundThreadExecutors.class);

  public BoundThreadExecutors(String name, int threads, ThreadFactory threadFactory) {
    this.name = name;
    this.threads = threads;
    this.threadFactory = threadFactory;
  }

  public void initialize() {
    executors = new SingleThreadExecutor[threads];
  }

  public Executor nextExecutor() {
    SingleThreadExecutor executor = executors[idx.getAndIncrement()%threads];
    executor.startThread();
    return executor;
  }

  public void stop() {
    for (SingleThreadExecutor executor : executors) {
      executor.interrupt();
    }
    boolean allStopped;
    int count = 0;
    while (true) {
      allStopped = true;
      for (SingleThreadExecutor executor : executors) {
        allStopped &= (executor.state.get() == STOPPED);
      }
      if (allStopped) {
        break;
      }
      if (count >= 5) {

        break;
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        logger.warn("interrupted when waiting for all executors stopping, " + name, e);
        break;
      }
      count++;
    }
    executors = null;
  }

  public static class SingleThreadExecutor {
    private Thread thread;
    private String name;
    private AtomicInteger state = new AtomicInteger(0);
    private BlockingQueue<Runnable> queue = new LinkedBlockingDeque<>();
    private Runnable parentTask = () -> {
      Runnable runnable;
      try {
        while (!Thread.currentThread().isInterrupted()) {
          runnable = queue.take();
          runnable.run();
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        if (logger.isDebugEnabled()) {
          logger.debug("thread interrupted. " + getName());
        }
      } finally {
        state.set(STOPPED);
      }
    };

    public SingleThreadExecutor(ThreadFactory threadFactory) {
      this.thread = threadFactory.newThread(parentTask);
      name = thread.getName();
    }

    private String getName() {
      return name;
    }

    public void interrupt() {
      thread.interrupt();
      state.set(STOPPING);
      thread = null;
      queue.clear();
      queue = null;
    }

    public void submit(TaskContext runnable) {
      try {
        queue.put(runnable);
      } catch (InterruptedException e) {
        throw new RuntimeException("cannot add task to thread " + thread.getName(), e);
      }
    }

    public boolean remove(Runnable runnable) {
      return queue.remove(runnable);
    }

    private void startThread() {
      if (state.get() == NOT_STARTED) {
        if (state.compareAndSet(NOT_STARTED, STARTING)) {
          try {
            thread.start();
          } finally {
            if (state.compareAndSet(STARTING, STARTED)) {
              throw new IllegalStateException("failed to start thread " + thread.getName());
            }
          }
        }
      }
    }
  }

}
