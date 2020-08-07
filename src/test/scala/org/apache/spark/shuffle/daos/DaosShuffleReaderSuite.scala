package org.apache.spark.shuffle.daos

import java.io.ByteArrayOutputStream
import java.util

import io.daos.BufferAllocator
import io.daos.obj.{DaosObject, IODataDesc}
import org.apache.spark._
import org.apache.spark.internal.config
import org.apache.spark.serializer.{JavaSerializer, SerializerManager}
import org.apache.spark.shuffle.BaseShuffleHandle
import org.apache.spark.storage.{BlockManagerId, ShuffleBlockId}
import org.mockito.Mockito.{doNothing, mock, when}
import org.mockito._

class DaosShuffleReaderSuite extends SparkFunSuite with LocalSparkContext {

  override def beforeAll(): Unit = {
    super.beforeAll()
    println("start executors in DaosReader " + classOf[DaosReader] + ", has executors: " + DaosReader.hasExecutors)
    MockitoAnnotations.initMocks(this)
  }

  private def mockObjectsForSingleDaosCall(reduceId: Int, numMaps: Int, byteOutputStream: ByteArrayOutputStream):
    (DaosReader, DaosShuffleIO, DaosObject) = {
    // mock
    val daosReader: DaosReader = Mockito.mock(classOf[DaosReader])
    val daosObject = Mockito.mock(classOf[DaosObject])
    val argumentCaptor = ArgumentCaptor.forClass(classOf[java.util.List[IODataDesc.Entry]])
    val shuffleIO = Mockito.mock(classOf[DaosShuffleIO])

    val desc = Mockito.mock(classOf[IODataDesc])
    when(daosObject.createDataDescForFetch(ArgumentMatchers.eq(String.valueOf(reduceId)), argumentCaptor.capture()))
      .thenReturn(desc)
    doNothing().when(daosObject).fetch(desc)
    when(desc.getNbrOfEntries()).thenReturn(numMaps)

    (0 until numMaps).foreach(i => {
      val entry = Mockito.mock(classOf[IODataDesc.Entry])
      when(desc.getEntry(i)).thenReturn(entry)
      val buf = BufferAllocator.objBufWithNativeOrder(byteOutputStream.size())
      buf.writeBytes(byteOutputStream.toByteArray)
      when(entry.getFetchedData).thenReturn(buf)
      when(entry.getKey).thenReturn(String.valueOf(i))
    })
    (daosReader, shuffleIO, daosObject)
  }

  private def mockObjectsForMultipleDaosCall(reduceId: Int, numMaps: Int, byteOutputStream: ByteArrayOutputStream,
                                             fromOtherThread: Boolean):
  (DaosReader, DaosShuffleIO, DaosObject) = {
    // mock
    val daosObject = Mockito.mock(classOf[DaosObject])
    val daosReader: DaosReader = if (fromOtherThread) Mockito.spy(new DaosReader(daosObject)) else Mockito.mock(classOf[DaosReader])
    val argumentCaptor = ArgumentCaptor.forClass(classOf[java.util.List[IODataDesc.Entry]])
    val shuffleIO = Mockito.mock(classOf[DaosShuffleIO])
    val descList = new util.ArrayList[IODataDesc]

    (0 until numMaps).foreach(_ => {
      descList.add(Mockito.mock(classOf[IODataDesc]))
    })
    val times = Array[Int](1)
    times(0) = 0
    when(daosObject.createDataDescForFetch(ArgumentMatchers.eq(String.valueOf(reduceId)), argumentCaptor.capture()))
      .thenAnswer(i => {
        val desc = descList.get(times(0))
        times(0) += 1
        desc
      })

    (0 until numMaps).foreach(i => {
      val desc = descList.get(i)
      doNothing().when(daosObject).fetch(desc)
      when(desc.getNbrOfEntries()).thenReturn(1)
      when(desc.succeeded()).thenReturn(true)
      when(desc.getTotalRequestSize).thenReturn(byteOutputStream.toByteArray.length)
      val entry = Mockito.mock(classOf[IODataDesc.Entry])
      when(desc.getEntry(0)).thenReturn(entry)
      val buf = BufferAllocator.objBufWithNativeOrder(byteOutputStream.size())
      buf.writeBytes(byteOutputStream.toByteArray)
      when(entry.getFetchedData).thenReturn(buf)
      when(entry.getKey).thenReturn(String.valueOf(i))
    })
    (daosReader, shuffleIO, daosObject)
  }

  private def testRead(keyValuePairsPerMap: Int, numMaps: Int, singleCall: Boolean = true,
                       fromOtherThread: Boolean = false): Unit = {
    val testConf = new SparkConf(false)
    testConf.set(SHUFFLE_DAOS_READ_FROM_OTHER_THREAD, fromOtherThread)

    // Create a buffer with some randomly generated key-value pairs to use as the shuffle data
    // from each mappers (all mappers return the same shuffle data).
    val serializer = new JavaSerializer(testConf)
    val byteOutputStream = new ByteArrayOutputStream()
    val serializationStream = serializer.newInstance().serializeStream(byteOutputStream)
    (0 until keyValuePairsPerMap).foreach { i =>
      serializationStream.writeKey(i)
      serializationStream.writeValue(2*i)
    }

    if (!singleCall) {
      val value = math.ceil(byteOutputStream.toByteArray.length.toDouble / 1024).toInt
      testConf.set(SHUFFLE_DAOS_READ_MAX_BYTES_IN_FLIGHT, value.toLong)
      testConf.set(SHUFFLE_DAOS_READ_MINIMUM_SIZE, value.toLong)
    }

    val reduceId = 15
    val shuffleId = 22
    // Create a SparkContext as a convenient way of setting SparkEnv (needed because some of the
    // shuffle code calls SparkEnv.get()).
    sc = new SparkContext("local", "test", testConf)

    // Make a mocked MapOutputTracker for the shuffle reader to use to determine what
    // shuffle data to read.
    val mapOutputTracker = mock(classOf[MapOutputTracker])
    val localBlockManagerId = BlockManagerId("test-client", "test-client", 1)
    when(mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, reduceId, reduceId + 1)).thenReturn {
      // Test a scenario where all data is local, to avoid creating a bunch of additional mocks
      // for the code to read data over the network.
      val shuffleBlockIdsAndSizes = (0 until numMaps).map { mapId =>
        val shuffleBlockId = ShuffleBlockId(shuffleId, mapId, reduceId)
        (shuffleBlockId, byteOutputStream.size().toLong, mapId)
      }
      Seq((localBlockManagerId, shuffleBlockIdsAndSizes)).toIterator
    }

    // Create a mocked shuffle handle to pass into HashShuffleReader.
    val shuffleHandle = {
      val dependency = mock(classOf[ShuffleDependency[Int, Int, Int]])
      when(dependency.serializer).thenReturn(serializer)
      when(dependency.aggregator).thenReturn(None)
      when(dependency.keyOrdering).thenReturn(None)
      new BaseShuffleHandle(shuffleId, dependency)
    }

    val serializerManager = new SerializerManager(
      serializer,
      new SparkConf()
        .set(config.SHUFFLE_COMPRESS, false)
        .set(config.SHUFFLE_SPILL_COMPRESS, false))

    val taskContext = TaskContext.empty()
    val metrics = taskContext.taskMetrics.createTempShuffleReadMetrics()
    val blocksByAddress = mapOutputTracker.getMapSizesByExecutorId(
      shuffleId, reduceId, reduceId + 1)

    val (daosReader, shuffleIO, daosObject) =
    if (singleCall) {
      mockObjectsForSingleDaosCall(reduceId, numMaps, byteOutputStream)
    } else {
      mockObjectsForMultipleDaosCall(reduceId, numMaps, byteOutputStream, fromOtherThread)
    }

    when(shuffleIO.getDaosReader(shuffleId)).thenReturn(daosReader)
    when(daosReader.getObject).thenReturn(daosObject)

    val shuffleReader = new DaosShuffleReader[Int, Int](
      shuffleHandle,
      blocksByAddress,
      taskContext,
      metrics,
      shuffleIO,
      serializerManager,
      false)

    assert(shuffleReader.read().length === keyValuePairsPerMap * numMaps)

    // verify metrics
    assert(metrics.remoteBytesRead === numMaps * byteOutputStream.toByteArray.length)
    println("remotes bytes: " + metrics.remoteBytesRead)
    assert(metrics.remoteBlocksFetched === numMaps)
  }

  test("test reader daos once") {
    testRead(10, 6)
  }

  test("test reader daos multiple times") {
    testRead(7168, 4, false)
  }

  test("test reader daos multiple times from other thread") {
    testRead(7168, 6, false, true)
  }
}
