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

package org.apache.spark.shuffle.daos

import io.daos.spark.DaosShuffleIO
import org.apache.spark.{MapOutputTracker, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.{BaseShuffleHandle, ShuffleReadMetricsReporter, ShuffleReader}
import org.apache.spark.storage.{BlockId, BlockManager, BlockManagerId}

class DaosShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    shuffleIO: DaosShuffleIO,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker,
    partitionId: Int,
    shouldBatchFetch: Boolean = false)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  private val daosReader = shuffleIO.getDaosReader(handle.shuffleId, partitionId);

  override def read(): Iterator[Product2[K, C]] = {

  }
}
