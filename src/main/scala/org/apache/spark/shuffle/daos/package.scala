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

package org.apache.spark.shuffle

import org.apache.spark.internal.config.ConfigBuilder
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.unsafe.array.ByteArrayMethods

package object daos {

  val SHUFFLE_DAOS_APP_ID = "spark.shuffle.daos.app.id"

  val SHUFFLE_DAOS_POOL_UUID =
    ConfigBuilder("spark.shuffle.daos.pool.uuid")
      .version("3.0.0")
      .stringConf
      .createWithDefault(null)

  val SHUFFLE_DAOS_CONTAINER_UUID =
    ConfigBuilder("spark.shuffle.daos.container.uuid")
      .version("3.0.0")
      .stringConf
      .createWithDefault(null)

  val SHUFFLE_DAOS_POOL_RANKS =
    ConfigBuilder("spark.shuffle.daos.ranks")
      .version("3.0.0")
      .stringConf
      .createWithDefault("0")

  val SHUFFLE_DAOS_PARTITION_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.daos.partition.buffer")
      .doc("Size of the in-memory buffer for each map partition output, in KiB")
      .version("3.0.0")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0 && v <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024,
        s"The map partition buffer size must be positive and less than or equal to" +
          s" ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024}.")
      .createWithDefaultString("2048k")

  val SHUFFLE_DAOS_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.daos.buffer")
      .doc("Size of total in-memory buffer for each map output, in MiB")
      .version("3.0.0")
      .bytesConf(ByteUnit.MiB)
      .checkValue(v => v > 50 && v <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024 / 1024,
        s"The total buffer size must be bigger than 50m and less than or equal to" +
          s" ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024 / 1024}.")
      .createWithDefaultString("800m")

  val SHUFFLE_DAOS_BUFFER_INITIAL_SIZE =
    ConfigBuilder("spark.shuffle.daos.buffer.initial")
      .doc("Initial size of total in-memory buffer for each map output, in MiB")
      .version("3.0.0")
      .bytesConf(ByteUnit.MiB)
      .checkValue(v => v > 10 && v <= ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024 / 1024,
        s"The initial total buffer size must be bigger than 10m and less than or equal to" +
          s" ${ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH / 1024 / 1024}.")
      .createWithDefaultString("80m")

  val SHUFFLE_DAOS_BUFFER_FORCE_WRITE_PCT =
    ConfigBuilder("spark.shuffle.daos.buffer.write.percentage")
      .doc("percentage of spark.shuffle.daos.buffer. Force write some buffer data out when size is bigger than " +
        "spark.shuffle.daos.buffer * (this percentage)")
      .version("3.0.0")
      .doubleConf
      .checkValue(v => v >= 0.5 && v <= 0.9,
        s"The percentage must be no less than 0.5 and less than or equal to 0.9")
      .createWithDefault(0.75)

  val SHUFFLE_DAOS_WRITE_MINIMUM_SIZE =
    ConfigBuilder("spark.shuffle.daos.write.minimum")
      .doc("minimum size when write to DAOS, in KiB. A warning will be generated when size is less than this value.")
      .version("3.0.0")
      .bytesConf(ByteUnit.KiB)
      .checkValue(v => v > 0,
        s"The DAOS write minimum size must be positive")
      .createWithDefaultString("128k")

  val SHUFFLE_DAOS_WRITE_SINGLE_BUFFER_SIZE =
    ConfigBuilder("spark.shuffle.daos.write.buffer.single")
      .doc("size of single buffer for holding data to be written to DAOS")
      .version("3.0.0")
      .bytesConf(ByteUnit.MiB)
      .checkValue(v => v >= 1,
        s"The single DAOS write buffer must be at least 1m")
      .createWithDefaultString("2m")

  val SHUFFLE_DAOS_READ_MINIMUM_SIZE =
    ConfigBuilder("spark.shuffle.daos.read.minimum")
      .doc("minimum size when read from DAOS, in MiB. ")
      .version("3.0.0")
      .bytesConf(ByteUnit.MiB)
      .checkValue(v => v > 0,
        s"The DAOS read minimum size must be positive")
      .createWithDefaultString("2m")

  val SHUFFLE_DAOS_READ_THREADS =
    ConfigBuilder("spark.shuffle.daos.read.threads")
      .doc("number of threads for each executor to read shuffle data concurrently. -1 means use number of executor " +
        "cores.")
      .version("3.0.0")
      .intConf
      .createWithDefault(1)

  val SHUFFLE_DAOS_READ_BATCH_SIZE =
    ConfigBuilder("spark.shuffle.daos.read.batch")
      .doc("number of read task to submit at each time")
      .version("3.0.0")
      .intConf
      .checkValue(v => v > 0,
        s"read batch size must be positive")
      .createWithDefault(10)

  val SHUFFLE_DAOS_READ_WAIT_DATA_MS =
    ConfigBuilder("spark.shuffle.daos.read.waitdata.ms")
      .doc("number of milliseconds to wait data being read from other thread")
      .version("3.0.0")
      .intConf
      .checkValue(v => v > 0,
        s"wait data time must be positive")
      .createWithDefault(100)

  val SHUFFLE_DAOS_READ_WAIT_DATA_TIMEOUT_TIMES =
    ConfigBuilder("spark.shuffle.daos.read.wait.timeout.times")
      .doc("number of wait timeout (spark.shuffle.daos.read.waitdata.ms) after which shuffle read task reads data " +
        "by itself instead of dedicated read thread")
      .version("3.0.0")
      .intConf
      .checkValue(v => v > 0,
        s"wait data timeout times must be positive")
      .createWithDefault(10)
}
