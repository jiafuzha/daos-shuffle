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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public abstract class LinkedTaskContext {

  protected final DaosObject object;
  protected final AtomicInteger counter;
  protected final Lock lock;
  protected final Condition condition;

  protected IODataDesc desc;
  protected LinkedTaskContext next;

  protected volatile boolean cancelled; // for multi-thread
  protected boolean cancelledByCaller; // for accessing by caller

  protected Object morePara;

  private static final Logger logger = LoggerFactory.getLogger(LinkedTaskContext.class);

  protected LinkedTaskContext(DaosObject object, AtomicInteger counter, Lock lock, Condition condition) {
    this.object = object;
    this.counter = counter;
    this.lock = lock;
    this.condition = condition;
  }

  protected void reuse(IODataDesc desc, Object morePara) {
    this.desc = desc;
    this.next = null;
    this.morePara = morePara;
    cancelled = false;
    cancelledByCaller = false;
  }

  protected void setNext(LinkedTaskContext next) {
    this.next = next;
  }

  protected LinkedTaskContext getNext() {
    return next;
  }

  protected IODataDesc getDesc() {
    return desc;
  }

  protected void signal() {
    counter.getAndIncrement();
    try {
      lock.lockInterruptibly();
      try {
        condition.signal();
      } finally {
        lock.unlock();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      logger.error("interrupted when signal task completion for " + desc, e);
    }
  }

  public void cancel() {
    cancelled = true;
    cancelledByCaller = true;
  }

  public boolean isCancelled() {
    return cancelledByCaller;
  }
}
