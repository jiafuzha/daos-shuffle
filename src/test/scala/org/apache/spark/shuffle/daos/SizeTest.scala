package org.apache.spark.shuffle.daos

import org.junit.Test

class SizeTest {

  def deCompressSize(size: Int): Long = {
    val LOG_BASE = 1.1
    val bytes = math.min(255, math.ceil(math.log(size) / math.log(LOG_BASE)).toInt).toByte
    val deSize = math.pow(LOG_BASE, bytes & 0xFF).toLong
    deSize
  }

  @Test
  def testCompressSize(): Unit = {
    (2 to 100).foreach(size => {
      val deSize = deCompressSize(size)
      if (size > deSize || deSize > size*1.1) {
        println(size)
      }
    })
  }

}
