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

package io.daos.spark;

import io.daos.DaosClient;
import io.daos.obj.DaosObjClient;
import io.daos.obj.DaosObject;
import io.daos.obj.DaosObjectException;
import io.daos.obj.DaosObjectId;
import org.apache.spark.SparkConf;
import org.apache.spark.shuffle.daos.package$;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DaosShuffleIO {

  private DaosObjClient objClient;

  private SparkConf conf;

  private long appId;

  private String execId;

  private Map<String, String> driverConf;

  private String poolId;

  private String contId;

  private String ranks;

  private List<DaosReader> readerList = new ArrayList<>();

  public DaosShuffleIO(SparkConf conf) {
    this.conf = conf;
  }

  private static final Logger logger = LoggerFactory.getLogger(DaosShuffleIO.class);

  public void initialize(long appId, String execId, Map<String, String> driverConf) throws IOException {
    this.appId = appId;
    this.execId = execId;
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

  public DaosWriter getDaosWriter(int shuffleId, long mapId, int bufferSize, int minSize) throws IOException {
    DaosObjectId id = new DaosObjectId(appId, shuffleId);
    id.encode();
    DaosObject object = objClient.getObject(id);
    object.open();
    return new DaosWriter(appId, shuffleId, mapId, bufferSize, minSize, object);
  }

  public DaosReader getDaosReader(int shuffleId, int reduceId) throws DaosObjectException {
    DaosObjectId id = new DaosObjectId(appId, shuffleId);
    id.encode();
    DaosObject object = objClient.getObject(id);
    object.open();
    DaosReader reader = new DaosReader(reduceId, object);
    readerList.add(reader);
    return reader;
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
    DaosReader.stopExecutor();
    objClient.forceClose();
    DaosClient.daosSafeFinalize();
  }
}
