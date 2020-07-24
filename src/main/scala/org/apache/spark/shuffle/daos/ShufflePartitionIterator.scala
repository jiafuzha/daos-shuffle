package org.apache.spark.shuffle.daos

import java.io.InputStream
import java.util
import java.util.concurrent.LinkedBlockingQueue

import io.daos.spark.{DaosInputStream, DaosReader}
import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.ShuffleReadMetricsReporter
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockId}
import org.apache.spark.util.TaskCompletionListener

import scala.collection.mutable

class ShufflePartitionIterator(
    context: TaskContext,
    blocksByAddress: Iterator[(BlockManagerId, Seq[(BlockId, Long, Int)])],
    streamWrapper: (BlockId, InputStream) => InputStream,
    maxBytesInFlight: Long,
    maxReqSizeShuffleToMem: Long,
    detectCorrupt: Boolean,
    detectCorruptUseExtraMemory: Boolean,
    shuffleMetrics: ShuffleReadMetricsReporter,
    daosReader: DaosReader,
    doBatchFetch: Boolean) extends Iterator[(BlockId, InputStream)] with Logging {

  private var nbrOfPartitionFetched = 0

  private var nbrOfPartitionProcessed = 0

  private var bytesInFlight = 0L

  private val corruptedBlocks = mutable.HashSet[BlockId]()

  private val results = new LinkedBlockingQueue[AnyRef]

  private var isZombie = false

  private val conf = SparkEnv.get.conf

  private val startTimeNs = System.nanoTime()

  private var lastBlockId: ShuffleBlockId = null

  private var inputStream: DaosInputStream = null

  private var done = false;

  private val onCompleteCallback = new ShufflePartitionCompletionListener(this)

  initialize

  def initialize: Unit = {
    context.addTaskCompletionListener(onCompleteCallback)
    startReading
  }

  def startReading: Unit = {
    val javaMap = new util.LinkedHashMap[Integer, java.lang.Long]()
    blocksByAddress.foreach(t2 => {
      t2._2.foreach(t3 => {
        if (javaMap.containsKey(t3._3)) {
          throw new IllegalStateException("duplicate map id: " + t3._3)
        }
        javaMap.put(t3._3, t3._2)
        lastBlockId = t3._1.asInstanceOf[ShuffleBlockId]
      })
    })
    inputStream = new DaosInputStream(daosReader, javaMap,
      maxBytesInFlight, maxReqSizeShuffleToMem)

  }

  override def hasNext: Boolean = {
    !done
  }

  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    done = true
    (lastBlockId, new BufferReleasingInputStream(inputStream))
  }
}

private class ShufflePartitionCompletionListener(var data: ShufflePartitionIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup()
      data = null
    }
  }
}
