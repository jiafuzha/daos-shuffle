package org.apache.spark.shuffle.daos

import org.apache.spark.TaskContext

object TaskContextObj {

  def emptyTaskContext: TaskContext = {
    TaskContext.empty()
  }

  def mergeReadMetrics(taskContext: TaskContext): Unit = {
    taskContext.taskMetrics().mergeShuffleReadMetrics()
  }
}
