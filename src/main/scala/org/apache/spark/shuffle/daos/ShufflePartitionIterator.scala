package org.apache.spark.shuffle.daos

import java.io.{IOException, InputStream}
import java.util

import org.apache.commons.io.IOUtils
import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle.{FetchFailedException, ShuffleReadMetricsReporter}
import org.apache.spark.storage.{BlockId, BlockManagerId, ShuffleBlockBatchId, ShuffleBlockId}
import org.apache.spark.util.{CompletionIterator, TaskCompletionListener, Utils}

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

  private var lastBlockId: ShuffleBlockId = null

  private[daos] var inputStream: DaosShuffleInputStream = null

  private var done = false

  private val onCompleteCallback = new ShufflePartitionCompletionListener(this)

  initialize

  def initialize: Unit = {
    context.addTaskCompletionListener(onCompleteCallback)
    startReading
  }

  private def getReduceId(shuffleBlockId: ShuffleBlockId): Int = {
    val name = shuffleBlockId.name
    name.substring(name.lastIndexOf('_') + 1).toInt
  }

  private def startReading: Unit = {
    // (mapid, reduceid) -> (length, BlockId, BlockManagerId)
    val javaMap = new util.LinkedHashMap[(Integer, Integer), (java.lang.Long, BlockId, BlockManagerId)]()
    blocksByAddress.foreach(t2 => {
      t2._2.foreach(t3 => {
        if (javaMap.containsKey(t3._3)) {
          throw new IllegalStateException("duplicate map id: " + t3._3)
        }
        javaMap.put((t3._3, getReduceId(t3._1.asInstanceOf[ShuffleBlockId])), (t3._2, t3._1, t2._1))
        lastBlockId = t3._1.asInstanceOf[ShuffleBlockId]
      })
    })

    if (log.isDebugEnabled) {
      log.debug(s"total mapreduceId: ${javaMap.size()}, they are, ")
      javaMap.forEach((key, value) => logDebug(key.toString() + " = " + value.toString))
    }

    inputStream = new DaosShuffleInputStream(daosReader, javaMap,
      maxBytesInFlight, maxReqSizeShuffleToMem, shuffleMetrics)
  }

  override def hasNext: Boolean = {
    !done
  }

  override def next(): (BlockId, InputStream) = {
    if (!hasNext) {
      throw new NoSuchElementException
    }
    done = true
    var input: InputStream = null
    var streamCompressedOrEncryptd = false
    try {
      input = streamWrapper(lastBlockId, inputStream)
      streamCompressedOrEncryptd = !input.eq(inputStream)
      if (streamCompressedOrEncryptd && detectCorruptUseExtraMemory) {
        input = Utils.copyStreamUpTo(input, maxBytesInFlight / 3)
      }
    } catch {
      case e: IOException =>
        logError(s"got an corrupted block ${inputStream.getCurBlockId} originated from " +
          s"${inputStream.getCurOriginAddress}.", e)
        throw e
    } finally {
      if (input == null) {
        inputStream.close()
      }
    }
    (lastBlockId, new BufferReleasingInputStream(input, this,
      detectCorrupt && streamCompressedOrEncryptd))
  }

  def throwFetchFailedException(
      blockId: BlockId,
      mapIndex: Int,
      address: BlockManagerId,
      e: Throwable) = {
    blockId match {
      case ShuffleBlockId(shufId, mapId, reduceId) =>
        throw new FetchFailedException(address, shufId, mapId, mapIndex, reduceId, e)
      case ShuffleBlockBatchId(shuffleId, mapId, startReduceId, _) =>
        throw new FetchFailedException(address, shuffleId, mapId, mapIndex, startReduceId, e)
      case _ =>
        throw new SparkException(
          "Failed to get block " + blockId + ", which is not a shuffle block", e)
    }
  }

  def toCompletionIterator: Iterator[(BlockId, InputStream)] = {
    CompletionIterator[(BlockId, InputStream), this.type](this,
      onCompleteCallback.onTaskCompletion(context))
  }

  def cleanup: Unit = {
    if (inputStream != null) {
      inputStream.close()
      inputStream = null;
    }
  }

}

/**
 * Helper class that ensures a ManagedBuffer is released upon InputStream.close() and
 * also detects stream corruption if streamCompressedOrEncrypted is true
 */
private class BufferReleasingInputStream(
                                          // This is visible for testing
                                          private val delegate: InputStream,
                                          private val iterator: ShufflePartitionIterator,
                                          private val detectCorruption: Boolean)
  extends InputStream {
  private[this] var closed = false

  override def read(): Int = {
    try {
      delegate.read()
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        val dinput = iterator.inputStream
        iterator.throwFetchFailedException(dinput.getCurBlockId, dinput.getCurMapIndex,
          dinput.getCurOriginAddress, e)
    }
  }

  override def close(): Unit = {
    if (!closed) {
      delegate.close()
      closed = true
    }
  }

  override def available(): Int = delegate.available()

  override def mark(readlimit: Int): Unit = delegate.mark(readlimit)

  override def skip(n: Long): Long = {
    try {
      delegate.skip(n)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        val dinput = iterator.inputStream
        iterator.throwFetchFailedException(dinput.getCurBlockId, dinput.getCurMapIndex,
          dinput.getCurOriginAddress, e)
    }
  }

  override def markSupported(): Boolean = delegate.markSupported()

  override def read(b: Array[Byte]): Int = {
    try {
      delegate.read(b)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        val dinput = iterator.inputStream
        iterator.throwFetchFailedException(dinput.getCurBlockId, dinput.getCurMapIndex,
          dinput.getCurOriginAddress, e)
    }
  }

  override def read(b: Array[Byte], off: Int, len: Int): Int = {
    try {
      delegate.read(b, off, len)
    } catch {
      case e: IOException if detectCorruption =>
        IOUtils.closeQuietly(this)
        val dinput = iterator.inputStream
        iterator.throwFetchFailedException(dinput.getCurBlockId, dinput.getCurMapIndex,
          dinput.getCurOriginAddress, e)
    }
  }

  override def reset(): Unit = delegate.reset()
}

private class ShufflePartitionCompletionListener(var data: ShufflePartitionIterator)
  extends TaskCompletionListener {

  override def onTaskCompletion(context: TaskContext): Unit = {
    if (data != null) {
      data.cleanup
    }
  }
}
