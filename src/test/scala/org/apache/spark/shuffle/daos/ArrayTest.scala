package org.apache.spark.shuffle.daos

import org.junit.Test

class ArrayTest {

  @Test
  def testJavaArray(): Unit = {
    val array = JavaTest.generateArray();
    array.foreach(v => println(v))
  }

  @Test
  def testScopeUpdate(): Unit = {
    var count = 0;
    (0 to 10).foreach(_ => count += 1)
    println(count)
  }

}
