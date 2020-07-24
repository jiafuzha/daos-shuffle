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

import java.util.concurrent.ConcurrentHashMap

import io.daos.{DaosClient, ShutdownHookManager}
import io.daos.spark.DaosShuffleIO
import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, config}
import org.apache.spark.shuffle.sort.SortShuffleManager.canUseBatchFetch
import org.apache.spark.shuffle._
import org.apache.spark.util.ShutdownHookManager
import org.apache.spark.util.collection.OpenHashSet

import collection.JavaConverters._;

/**
 * A shuffle manager to write and read map data from DAOS using DAOS object API.
 *
 * @param conf
 * spark configuration
 */
class DaosShuffleManager(conf: SparkConf) extends ShuffleManager with Logging {

  if (SparkEnv.get.conf.get(config.SHUFFLE_USE_OLD_FETCH_PROTOCOL)) {
    throw new IllegalArgumentException("DaosShuffleManager doesn't support old fetch protocol. Please remove " +
      config.SHUFFLE_USE_OLD_FETCH_PROTOCOL.key)
  }

  def parseAppId(appId: String): Long = {
    appId.map(c => if (c.isDigit) c else 0.toChar).toLong
  }

  val appId = parseAppId(conf.getAppId)
  conf.set(SHUFFLE_DAOS_APP_ID, appId.toString)
  logInfo(s"application id: ${appId}")

  if (io.daos.ShutdownHookManager.removeHook(DaosClient.FINALIZER)) {
    ShutdownHookManager.addHook(DaosClient.FINALIZER)
  }

  val daosShuffleIO = new DaosShuffleIO(conf)
  daosShuffleIO.initialize(
    appId,
    SparkEnv.get.executorId,
    conf.getAllWithPrefix(ShuffleDataIOUtils.SHUFFLE_SPARK_CONF_PREFIX).toMap.asJava)


  /**
   * A mapping from shuffle ids to the task ids of mappers producing output for those shuffles.
   */
  private[this] val taskIdMapsForShuffle = new ConcurrentHashMap[Int, OpenHashSet[Long]]()

  /**
   * register [[ShuffleDependency]] to pass to tasks.
   *
   * @param shuffleId
   * unique ID of shuffle in job
   * @param dependency
   * shuffle dependency
   * @tparam K
   * type of KEY
   * @tparam V
   * type of VALUE
   * @tparam C
   * type of combined value
   * @return [[BaseShuffleHandle]]
   */
  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C])

    = {
    new BaseShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long,
      context: TaskContext,
      metrics: ShuffleWriteMetricsReporter)

    = {
    val mapTaskIds = taskIdMapsForShuffle.computeIfAbsent(
      handle.shuffleId, _ => new OpenHashSet[Long](16))
    mapTaskIds.synchronized { mapTaskIds.add(context.taskAttemptId()) }
    new DaosShuffleWriter(handle.asInstanceOf[BaseShuffleHandle[K, V, _]], mapId, context, daosShuffleIO)
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter)

    = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startPartition, endPartition)
    new DaosShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context,
      metrics, daosShuffleIO, shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  override def getReaderForRange[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter)

    = {
    val blocksByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByRange(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition)
    new DaosShuffleReader(handle.asInstanceOf[BaseShuffleHandle[K, _, C]], blocksByAddress, context,
      metrics, daosShuffleIO, shouldBatchFetch = canUseBatchFetch(startPartition, endPartition, context))
  }

  override def unregisterShuffle(shuffleId: Int)

    = {
    Option(taskIdMapsForShuffle.remove(shuffleId)).foreach { mapTaskIds =>
      mapTaskIds.iterator.foreach { mapTaskId =>
        // TODO: remove data from DAOS, object punch here
        }
    }
    true
  }

  override def shuffleBlockResolver

    = null

  override def stop(): Unit = {
    daosShuffleIO.close()
  }
}
