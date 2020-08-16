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

package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObjClient;
import io.daos.obj.DaosObject;
import io.daos.obj.DaosObjectException;
import io.daos.obj.DaosObjectId;
import org.apache.spark.SparkConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DaosShuffleIO {

  private DaosObjClient objClient;

  private SparkConf conf;

  private Map<String, String> driverConf;

  private String poolId;

  private String contId;

  private String ranks;

  private boolean warnSmallWrite;

  private List<DaosReader> readerList = new ArrayList<>();

  private Map<String, DaosObject> objectMap = new ConcurrentHashMap<>();

  public DaosShuffleIO(SparkConf conf) {
    this.conf = conf;
    this.warnSmallWrite = (boolean)conf.get(package$.MODULE$.SHUFFLE_DAOS_WRITE_WARN_SMALL_SIZE());
  }

  private static final Logger logger = LoggerFactory.getLogger(DaosShuffleIO.class);

  public void initialize(Map<String, String> driverConf) throws IOException {
    this.driverConf = driverConf;
    poolId = conf.get(package$.MODULE$.SHUFFLE_DAOS_POOL_UUID());
    contId = conf.get(package$.MODULE$.SHUFFLE_DAOS_CONTAINER_UUID());
    ranks = conf.get(package$.MODULE$.SHUFFLE_DAOS_POOL_RANKS());
    if (poolId == null || contId == null) {
      throw new IllegalArgumentException("DaosShuffleManager needs pool id and container id");
    }

    objClient = new DaosObjClient.DaosObjClientBuilder()
      .poolId(poolId).containerId(contId).ranks(ranks)
      .build();
  }

  private long parseAppId(String appId) {
    return Long.valueOf(appId.replaceAll("\\D", ""));
  }

  /**
   * Should be called in the Driver after TaskScheduler registration and
   * from the start in the Executor.
   *
   * @param numPartitions
   * @param shuffleId
   * @param mapId
   * @param bufferSize
   * @param minSize
   * @return
   * @throws IOException
   */
  public DaosWriter getDaosWriter(int numPartitions, int shuffleId, long mapId, int bufferSize, int minSize)
      throws IOException {
    long appId = parseAppId(conf.getAppId());
    if (logger.isDebugEnabled()) {
      logger.debug("getting daoswriter for app id: " + appId + ", shuffle id: " + shuffleId + ", map id: " + mapId +
          ", numPartitions: " + numPartitions);
    }
    DaosWriter writer = new DaosWriter(appId, numPartitions, shuffleId, mapId, bufferSize, minSize,
        getObject(appId, shuffleId));
    writer.setWarnSmallWrite(warnSmallWrite);
    return writer;
  }

  public DaosReader getDaosReader(int shuffleId) throws DaosObjectException {
    long appId = parseAppId(conf.getAppId());
    if (logger.isDebugEnabled()) {
      logger.debug("getting daosreader for app id: " + appId + ", shuffle id: " + shuffleId);
    }
    DaosReader reader = new DaosReader(getObject(appId, shuffleId));
    readerList.add(reader);
    return reader;
  }

  private String getKey(long appId, int shuffleId) {
    return appId + "" + shuffleId;
  }

  private DaosObject getObject(long appId, int shuffleId) throws DaosObjectException {
    String key = getKey(appId, shuffleId);
    DaosObject object = objectMap.get(key);
    if (object == null) {
      DaosObjectId id = new DaosObjectId(appId, shuffleId);
      id.encode();
      object = objClient.getObject(id);
      objectMap.putIfAbsent(key, object);
      DaosObject activeObject = objectMap.get(key);
      if (activeObject != object) { // release just created DaosObject
        object.close();
        object = activeObject;
      }
      // open just once in multiple threads
      if (!object.isOpen()) {
        synchronized (object) {
          object.open();
        }
      }
    }
    return object;
  }

  public boolean removeShuffle(int shuffleId) {
    long appId = parseAppId(conf.getAppId());
    logger.info("punching daos object for app id: " + appId + ", shuffle id: " + shuffleId);
    try {
      DaosObject object = objectMap.remove(getKey(appId, shuffleId));
      if (object != null) {
        object.punch();
        object.close();
      }
    } catch (Exception e) {
      logger.error("failed to punch object", e);
      return false;
    }
    return true;
  }

  public void close() throws IOException {
    readerList.forEach(r -> {
      try {
        r.close();
      } catch (IOException e) {
        logger.warn("failed to close reader " + r.toString());
      }
    });
    readerList.clear();
    objClient.forceClose();
  }
}
