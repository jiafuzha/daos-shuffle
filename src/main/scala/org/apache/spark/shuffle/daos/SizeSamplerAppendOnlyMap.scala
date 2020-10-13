package org.apache.spark.shuffle.daos

import org.apache.spark.util.collection.AppendOnlyMap

private[spark] class SizeSamplerAppendOnlyMap[K, V](val stat: SampleStat)
  extends AppendOnlyMap[K, V] with SizeSampler
{

  setSampleStat(stat)
  resetSamples()

  override def update(key: K, value: V): Unit = {
    super.update(key, value)
    super.afterUpdate()
  }

  override def changeValue(key: K, updateFunc: (Boolean, V) => V): V = {
    val newValue = super.changeValue(key, updateFunc)
    super.afterUpdate()
    newValue
  }

  override protected def growTable(): Unit = {
    super.growTable()
    resetSamples()
  }
}
