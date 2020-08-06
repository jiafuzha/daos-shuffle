package org.apache.spark.shuffle.daos;

import io.daos.obj.DaosObjClient;
import io.daos.obj.DaosObject;
import io.daos.obj.DaosObjectId;
import io.daos.obj.IODataDesc;
import io.netty.buffershade5.ByteBuf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.shuffle.ShuffleReadMetricsReporter;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.BlockManagerId;
import org.apache.spark.storage.ShuffleBlockId;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.SuppressStaticInitializationFor;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@SuppressStaticInitializationFor("io.daos.obj.DaosObjClient")
public class DaosShuffleInputStreamTest {

  private static final Logger LOG = LoggerFactory.getLogger(DaosShuffleInputStreamTest.class);

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes1() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("4", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("30", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes2() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("0", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("30", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes3() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("4", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("40", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelMultipleTimes4() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("0", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("40", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  @Test
  public void testReadFromOtherThreadCancelAll() throws Exception {
    Map<String, AtomicInteger> maps = new HashMap<>();
    maps.put("4", new AtomicInteger(0));
    maps.put("7", new AtomicInteger(0));
    maps.put("12", new AtomicInteger(0));
    maps.put("24", new AtomicInteger(0));
    maps.put("30", new AtomicInteger(0));
    maps.put("40", new AtomicInteger(0));
    readFromOtherThreadCancelMultipleTimes(maps);
  }

  public void readFromOtherThreadCancelMultipleTimes(Map<String, AtomicInteger> maps) throws Exception {
    int waitDataTimeMs = (int)new SparkConf(false).get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_MS());
    int expectedFetchTimes = 32;
    AtomicInteger fetchTimes = new AtomicInteger(0);
    boolean[] succeeded = new boolean[] {true};
    Method method = IODataDesc.class.getDeclaredMethod("succeed");
    method.setAccessible(true);
    CountDownLatch latch = new CountDownLatch(expectedFetchTimes);

    Answer<InvocationOnMock> answer = (invocationOnMock ->
    {
      fetchTimes.getAndIncrement();
      IODataDesc desc = invocationOnMock.getArgument(0);
      desc.encode();
      method.invoke(desc);

      IODataDesc.Entry entry = desc.getEntry(0);
      String mapId = entry.getKey();
      if (maps.containsKey(mapId)) {
        AtomicInteger wait = maps.get(mapId);
        if (wait.get() == 0) {
          wait.incrementAndGet();
          Thread.sleep(waitDataTimeMs + 50);
          System.out.println("sleep at " + mapId);
          return invocationOnMock;
        } else {
          latch.countDown();
          setLength(desc, succeeded, null);
          System.out.println("self read later at " + mapId);
          return invocationOnMock;
        }
      }
      latch.countDown();
      setLength(desc, succeeded, null);
      return invocationOnMock;
    });
    read(42, answer, latch, fetchTimes, succeeded);
  }

  @Test
  public void testReadFromOtherThreadCancelOnceAtLast() throws Exception {
    testReadFromOtherThreadCancelOnce(40, 10240);
  }

  @Test
  public void testReadFromOtherThreadCancelOnceAtMiddle() throws Exception {
    testReadFromOtherThreadCancelOnce(2, 0);
  }

  @Test
  public void testReadFromOtherThreadCancelOnceAtFirst() throws Exception {
    testReadFromOtherThreadCancelOnce(0, 10*1024);
  }

  private void testReadFromOtherThreadCancelOnce(int pos, int desiredOffset) throws Exception {
    int waitDataTimeMs = (int)new SparkConf(false).get(package$.MODULE$.SHUFFLE_DAOS_READ_WAIT_DATA_MS());
    int expectedFetchTimes = 32;
    AtomicInteger fetchTimes = new AtomicInteger(0);
    boolean[] succeeded = new boolean[] {true};
    AtomicInteger wait = new AtomicInteger(0);
    Method method = IODataDesc.class.getDeclaredMethod("succeed");
    method.setAccessible(true);
    CountDownLatch latch = new CountDownLatch(expectedFetchTimes);

    Answer<InvocationOnMock> answer = (invocationOnMock ->
    {
      fetchTimes.getAndIncrement();
      IODataDesc desc = invocationOnMock.getArgument(0);
      desc.encode();
      method.invoke(desc);

      IODataDesc.Entry entry = desc.getEntry(0);
      int offset = entry.getOffset();
      if (String.valueOf(pos).equals(entry.getKey()) && offset == desiredOffset) {
        if (wait.get() == 0) {
          wait.incrementAndGet();
          Thread.sleep(waitDataTimeMs + 50);
          System.out.println("sleep at " + pos);
          return invocationOnMock;
        } else {
          latch.countDown();
          checkAndSetSize(desc, succeeded, (15 * 1024 - desiredOffset), (5 * 1024) + desiredOffset);
          System.out.println("self read later at " + pos);
          return invocationOnMock;
        }
      }
      latch.countDown();
      setLength(desc, succeeded, null);
      return invocationOnMock;
    });
    read(42, answer, latch, fetchTimes, succeeded);
  }

  @Test
  public void testReadSmallMapFromOtherThread() throws Exception {
    int expectedFetchTimes = 32;
    AtomicInteger fetchTimes = new AtomicInteger(0);
    boolean[] succeeded = new boolean[] {true};
    Method method = IODataDesc.class.getDeclaredMethod("succeed");
    method.setAccessible(true);
    CountDownLatch latch = new CountDownLatch(expectedFetchTimes);

    Answer<InvocationOnMock> answer = (invocationOnMock ->
    {
      fetchTimes.getAndIncrement();
      IODataDesc desc = invocationOnMock.getArgument(0);
      desc.encode();
      method.invoke(desc);

      IODataDesc.Entry entry = desc.getEntry(0);
      int offset = entry.getOffset();
      if ("0".equals(entry.getKey()) && offset == 0) {
        latch.countDown();
        checkAndSetSize(desc, succeeded, 10*1024);
        return invocationOnMock;
      }
      if ("0".equals(entry.getKey()) && offset == 10*1024) {
        latch.countDown();
        checkAndSetSize(desc, succeeded, 5*1024, 15*1024);
        return invocationOnMock;
      }
      if ("0".equals(entry.getKey()) && offset == 30*1024) {
        latch.countDown();
        checkAndSetSize(desc, succeeded, 15*1024, 5*1024);
        return invocationOnMock;
      }
      latch.countDown();
      setLength(desc, succeeded, null);
      return invocationOnMock;
    });
    read(42, answer, latch, fetchTimes, succeeded);
  }

  private void read(int maps, Answer<InvocationOnMock> answer,
                    CountDownLatch latch, AtomicInteger fetchTimes, boolean[] succeeded) throws Exception {
    UserGroupInformation.setLoginUser(UserGroupInformation.createRemoteUser("test"));
    SparkConf testConf = new SparkConf(false);
    long minSize = 10;
    testConf.set(package$.MODULE$.SHUFFLE_DAOS_READ_MINIMUM_SIZE(), minSize);
    SparkContext context = new SparkContext("local", "test", testConf);
    TaskContext taskContext = TaskContextObj.emptyTaskContext();
    ShuffleReadMetricsReporter metrics = taskContext.taskMetrics().createTempShuffleReadMetrics();
    // daos object mock
    DaosObjectId id = Mockito.mock(DaosObjectId.class);
    Mockito.when(id.isEncoded()).thenReturn(true);
    DaosObjClient client = PowerMockito.mock(DaosObjClient.class);
    Constructor<DaosObject> objectConstructor =
        DaosObject.class.getDeclaredConstructor(DaosObjClient.class, DaosObjectId.class);
    objectConstructor.setAccessible(true);
    DaosObject daosObject = Mockito.spy(objectConstructor.newInstance(client, id));

    Mockito.doAnswer(answer).when(daosObject).fetch(any(IODataDesc.class));

    DaosReader daosReader = new DaosReader(daosObject);
    LinkedHashMap<Tuple2<Integer, Integer>, Tuple3<Long, BlockId, BlockManagerId>> partSizeMap = new LinkedHashMap<>();
    int shuffleId = 10;
    int reduceId = 1;
    int size = (int)(minSize + 5) * 1024;
    for (int i = 0; i < maps; i++) {
      partSizeMap.put(new Tuple2<>(i, 10), new Tuple3<>(Long.valueOf(size), new ShuffleBlockId(shuffleId, i, reduceId),
          BlockManagerId.apply("1", "localhost", 2, Option.empty())));
    }

    DaosShuffleInputStream is = new DaosShuffleInputStream(daosReader, partSizeMap, 2 * minSize * 1024,
        2 * 1024 * 1024, metrics);
    try {
      // verify cancelled task and continuing submission
      for (int i = 0; i < maps; i++) {
        byte[] bytes = new byte[size];
        is.read(bytes);
        for (int j = 0; j < 255; j++) {
          try {
            Assert.assertEquals((byte) j, bytes[j]);
          } catch (Throwable e) {
            LOG.error("error at map " + i + ", loc: " + j);
            throw e;
          }
        }
        Assert.assertEquals(-1, is.read());
        is.nextMap();
      }
      boolean alldone = latch.await(5, TimeUnit.SECONDS);
      System.out.println(fetchTimes.get());
      Assert.assertTrue(alldone);
      Assert.assertTrue(succeeded[0]);
      TaskContextObj.mergeReadMetrics(taskContext);
      System.out.println(taskContext.taskMetrics().shuffleReadMetrics()._fetchWaitTime().sum());
    } finally {
      daosReader.close();
      DaosReader.stopExecutor();
      is.close(true);
      context.stop();
    }
  }

  private void checkAndSetSize(IODataDesc desc, boolean[] succeeded, int... sizes) {
    if (desc.getNbrOfEntries() != sizes.length) {
      succeeded[0] = false;
      throw new AssertionError("number of entries should be " + sizes.length +", not " + desc.getNbrOfEntries());
    }

    setLength(desc, succeeded, sizes);
  }

  private void setLength(IODataDesc desc, boolean[] succeeded, int[] sizes) {
    for (int i = 0; i < desc.getNbrOfEntries(); i++) {
      ByteBuf buf = desc.getEntry(i).getFetchedData();
      if (sizes != null) {
        if (buf.capacity() != sizes[i]) {
          succeeded[0] = false;
          throw new AssertionError("buf capacity should be " + sizes[i] + ", not " + buf.capacity());
        }
      }
      for (int j = 0; j < buf.capacity(); j++) {
        buf.writeByte((byte)j);
      }
    }
  }
}
