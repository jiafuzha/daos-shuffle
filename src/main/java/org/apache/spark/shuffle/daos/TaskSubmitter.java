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

import io.daos.obj.IODataDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Deque;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public abstract class TaskSubmitter {
  protected final BoundThreadExecutors.SingleThreadExecutor executor;

  protected LinkedTaskContext headCtx;
  protected LinkedTaskContext currentCtx;
  protected LinkedTaskContext lastCtx;
  protected Deque<LinkedTaskContext> consumedStack = new LinkedList<>();

  protected Lock lock = new ReentrantLock();
  protected Condition condition = lock.newCondition();
  protected AtomicInteger counter = new AtomicInteger(0);

  protected int exceedWaitTimes;
  protected long totalInMemSize;
  protected int totalSubmitted;

  private static final Logger log = LoggerFactory.getLogger(TaskSubmitter.class);
  
  protected TaskSubmitter(BoundThreadExecutors.SingleThreadExecutor executor) {
    this.executor = executor;
  }

  protected void submit(IODataDesc taskDesc, Object morePara) {
    LinkedTaskContext context = tryReuseContext(taskDesc, morePara);
    executor.execute(newTask(context));
    totalInMemSize += taskDesc.getTotalRequestSize();
    totalSubmitted++;
  }

  protected abstract Runnable newTask(LinkedTaskContext context);

  /**
   * wait for condition to be meet.
   *
   * @param waitDataTimeMs
   * @return true for timeout, false otherwise.
   * @throws {@link InterruptedException}
   */
  protected boolean waitForCondition(long waitDataTimeMs) throws InterruptedException {
    lock.lockInterruptibly();
    try {
      if (!condition.await(waitDataTimeMs, TimeUnit.MILLISECONDS)) {
        return true;
      }
      return false;
    } finally {
      lock.unlock();
    }
  }

  protected LinkedTaskContext tryReuseContext(IODataDesc desc, Object morePara) {
    LinkedTaskContext context = consumedStack.poll();
    if (context != null) {
      context.reuse(desc, morePara);
    } else {
      context = createTaskContext(desc, morePara);
    }
    if (lastCtx != null) {
      lastCtx.setNext(context);
    }
    lastCtx = context;
    if (headCtx == null) {
      headCtx = context;
    }
    return context;
  }

  protected abstract LinkedTaskContext createTaskContext(IODataDesc desc, Object morePara);

  protected boolean moveForward(boolean restore) throws IOException {
    int c;
    while ((c = counter.decrementAndGet()) >= 0) {
      restore = true; // we got at least one. Thus, we need to restore negative value.
      if (consume()) {
        return true;
      }
    }
    if (c < 0) {
      if (!restore && log.isDebugEnabled()) {
        log.debug("spurious wakeup");
      }
      counter.incrementAndGet();
    }
    return false;
  }

  private boolean consume() throws IOException {
    if (currentCtx != null) {
      totalInMemSize -= currentCtx.getDesc().getTotalRequestSize();
      if (consumed(currentCtx)) { // no context reuse since selfCurrentCtx could reference it
        consumedStack.offer(currentCtx);
      }
      currentCtx = currentCtx.getNext();
    } else { // returned first time
      currentCtx = headCtx;
    }
    totalSubmitted -= 1;
    return validateReturned(currentCtx);
  }

  protected abstract boolean validateReturned(LinkedTaskContext context) throws IOException;

  protected abstract boolean consumed(LinkedTaskContext context);

  protected void cancelTasks(boolean cancelAll) {
    LinkedTaskContext ctx = getNextNonReturnedCtx();
    if (ctx == null) { // reach to end
      return;
    }
    ctx.cancel();
    if (cancelAll) {
      ctx = ctx.getNext();
      while (ctx != null) {
        ctx.cancel();
        ctx = ctx.getNext();
      }
    }
  }

  protected LinkedTaskContext getNextNonReturnedCtx() {
    LinkedTaskContext ctx = currentCtx;
    if (ctx == null) {
      return getHeadCtx();
    }
    return ctx.getNext();
  }

  protected boolean cleanupSubmitted(boolean force) {
    boolean allReleased = true;
    LinkedTaskContext ctx = currentCtx;
    while (ctx != null) {
      if (!ctx.isCancelled()) {
        ctx.cancel();
      }
      allReleased &= cleanupTaskContext(ctx, force);
      ctx = ctx.getNext();
    }
    return allReleased;
  }

  protected boolean cleanupConsumed(boolean force) {
    boolean allReleased = true;
    for (LinkedTaskContext ctx : consumedStack) {
      allReleased &= cleanupTaskContext(ctx, force);
    }
    if (allReleased) {
      consumedStack.clear();
    }
    return allReleased;
  }

  protected boolean cleanupTaskContext(LinkedTaskContext ctx, boolean force) {
    if (ctx == null) {
      return true;
    }
    IODataDesc desc = ctx.getDesc();
    if (desc != null && (force || desc.succeeded())) {
      desc.release();
      return true;
    }
    return false;
  }

  public LinkedTaskContext getCurrentCtx() {
    return currentCtx;
  }

  public LinkedTaskContext getHeadCtx() {
    return headCtx;
  }

  public LinkedTaskContext getLastCtx() {
    return lastCtx;
  }
}
