package org.apache.spark.shuffle.daos

import org.apache.spark.util.SizeEstimator

import scala.collection.mutable

/**
 * A trait to sample size of object. It mimics what {@link SizeTracker} does. The differences between them are,
 * - this trait is for sampling size of each partition buffer.
 * - this trait lets caller control when to sample size.
 * - bytesPerUpdate is calculated and shared among all buffers.
 */
private[spark] trait SizeSampler {

  import SizeSampler._

  /** Samples taken since last resetSamples(). Only the last two are kept for extrapolation. */
  private val samples = new mutable.Queue[Sample]

  /** Total number of insertions and updates into the map since the last resetSamples(). */
  private var numUpdates: Long = _

  private var bytesPerUpdate: Double = _

  private var stat: SampleStat = _

  protected def setSampleStat(stat: SampleStat): Unit = {
    this.stat = stat
  }

  /**
   * Reset samples collected so far.
   * This should be called after the collection undergoes a dramatic change in size.
   */
  protected def resetSamples(): Unit = {
    numUpdates = 1
    samples.clear()
    takeSample()
  }

  protected def afterUpdate(): Unit = {
    numUpdates += 1
    stat.incNumUpdates
    if (stat.needSample) {
      takeSample()
    }
  }

  /**
   * Take a new sample of the current collection's size.
   */
  protected def takeSample(): Unit = {
    samples.enqueue(Sample(SizeEstimator.estimate(this), numUpdates))
    // Only use the last two samples to extrapolate
    if (samples.size > 2) {
      samples.dequeue()
    }
    var updateDelta = 0L
    val bytesDelta = samples.toList.reverse match {
      case latest :: previous :: _ =>
        updateDelta = latest.numUpdates - previous.numUpdates
        (latest.size - previous.size).toDouble / updateDelta
      // If fewer than 2 samples, assume no change
      case _ => 0
    }
    if (updateDelta == 0) {
      return
    }
    bytesPerUpdate = math.max(0, bytesDelta)
    stat.updateStat(bytesPerUpdate, updateDelta)
  }

  /**
   * Estimate the current size of the collection in bytes. O(1) time.
   */
  def estimateSize(): Long = {
    assert(samples.nonEmpty)
    val bpu = if (bytesPerUpdate == 0) stat.bytesPerUpdate else bytesPerUpdate
    val extrapolatedDelta = bpu * (numUpdates - samples.last.numUpdates)
    (samples.last.size + extrapolatedDelta).toLong
  }
}

private[spark] class SampleStat
{
  /**
   * Controls the base of the exponential which governs the rate of sampling.
   * E.g., a value of 2 would mean we sample at 1, 2, 4, 8, ... elements.
   */
  private val SAMPLE_GROWTH_RATE = 1.1

  private[daos] var numUpdates: Long = 0
  private[daos] var lastNumUpdates: Long = 0
  private[daos] var nextSampleNum: Long = 1
  private[daos] var bytesPerUpdate: Double = 0

  def updateStat(partBpu: Double, partUpdateDelta: Long) = {
    bytesPerUpdate = ((numUpdates - partUpdateDelta) * bytesPerUpdate +
                        partUpdateDelta * partBpu
                      ) / numUpdates
    lastNumUpdates = numUpdates
    nextSampleNum = math.ceil(numUpdates * SAMPLE_GROWTH_RATE).toLong
  }

  def needSample: Boolean = {
    numUpdates == nextSampleNum
  }

  def incNumUpdates: Unit = {
    numUpdates += 1
  }
}

private object SizeSampler {
  case class Sample(size: Long, numUpdates: Long)
}
