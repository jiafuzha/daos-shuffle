package org.apache.spark.shuffle.daos

import org.apache.spark.SparkFunSuite

import scala.util.Random

class SizeSamplerSuite extends SparkFunSuite {

  test("test sample AppendOnlyMap by update") {
    val stat = new SampleStat
    var grew = false;
    val map = new SizeSamplerAppendOnlyMap[Int, Int](stat) {
      override def growTable(): Unit = {
        super.growTable()
        grew = true
      }
    }
    (1 to 65).foreach {
      i => map.update(i, i)
//      println(map.estimateSize())
//      println(stat.bytesPerUpdate)
      assert(i === stat.numUpdates)
      assert(stat.lastNumUpdates <= stat.numUpdates)
      assert(stat.nextSampleNum >= stat.numUpdates)
      if (i == 15) {
        assert(stat.nextSampleNum === 17)
      }
      if (i == 45) {
        assert(grew === true)
      }
//      println("==========")
    }
  }

  test("test sample AppendOnlyMap by changeValue") {
    val stat = new SampleStat
    var grew = false;
    val map = new SizeSamplerAppendOnlyMap[Int, Int](stat) {
      override def growTable(): Unit = {
        super.growTable()
        grew = true
      }
    }
    val updateFun = (exist: Boolean, v: Int) => {
      new Random().nextInt(100) + v
    }
    (1 to 65).foreach {
      i => map.changeValue(i, updateFun)
//      println(map.estimateSize())
//      println(stat.bytesPerUpdate)
      assert(i === stat.numUpdates)
      assert(stat.lastNumUpdates <= stat.numUpdates)
      assert(stat.nextSampleNum >= stat.numUpdates)
      if (i == 15) {
        assert(stat.nextSampleNum === 17)
      }
      if (i == 45) {
        assert(grew === true)
      }
//      println("==========")
    }
  }

  test("test sample PairBuffer by insert") {
    val stat = new SampleStat
    var grew = false;
    val buffer = new SizeSamplerPairBuffer[Int, Int](stat) {
      override def growArray(): Unit = {
        super.growArray()
        grew = true
      }
    }
    (1 to 73).foreach {
      i => buffer.insert(i, i)
//      println(buffer.estimateSize())
//      println(stat.bytesPerUpdate)
      assert(i === stat.numUpdates)
      assert(stat.lastNumUpdates <= stat.numUpdates)
      assert(stat.nextSampleNum >= stat.numUpdates)
      if (i == 15) {
        assert(stat.nextSampleNum === 17)
      }
      if (i == 65) {
        assert(grew === true)
      }
//      println("==========")
    }
  }
}
