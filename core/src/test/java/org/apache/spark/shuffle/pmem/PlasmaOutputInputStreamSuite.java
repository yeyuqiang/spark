/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.pmem;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkEnv;
import org.apache.spark.network.buffer.ManagedBuffer;
import org.apache.spark.serializer.*;
import org.apache.spark.storage.BlockId;
import org.apache.spark.storage.ShuffleBlockId;
import org.junit.*;
import scala.Tuple2;
import scala.collection.Iterator;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assume.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests functionality of {@link PlasmaInputStream} and {@link PlasmaOutputStream}
 */
public class PlasmaOutputInputStreamSuite {

  private final Random random = new Random();

  @BeforeClass
  public static void setUp() {
    mockSparkEnv();

    boolean isAvailable = PlasmaStoreServer.isPlasmaJavaAvailable();
    assumeTrue("Please make sure libplasma_java.so is installed" +
        " under LD_LIBRARY_PATH or java.library.path", isAvailable);

    boolean isExist = PlasmaStoreServer.isPlasmaStoreExist();
    assumeTrue("Please make sure plasma store server is installed " +
        "and added to the System PATH before run these unit tests", isExist);
    try {
      boolean isStarted = PlasmaStoreServer.startPlasmaStore();
      assumeTrue("Failed to start plasma store server", isStarted);
    } catch (IOException ex1) {
      ex1.printStackTrace();
    } catch (InterruptedException ex2) {
      ex2.printStackTrace();
    }

  }

  @Test
  public void basicPlasmaClientOperation() {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] chunkToWrite = prepareByteBlockToWrite(1);

    MyPlasmaClient client = MyPlasmaClientHolder.get();

    client.writeChunk(blockId, 0, chunkToWrite);
    ByteBuffer buf = client.readChunk(blockId, 0);
    for (byte b : chunkToWrite) {
      assertEquals(b, buf.get());
    }

    PlasmaObjectId objectId = new PlasmaObjectId(blockId, 0);
    client.delete(objectId.toBytes());
    ByteBuffer bufAfterDel = client.readChunk(blockId, 0);
    assertNull(bufAfterDel);

  }

  @Test(expected = NullPointerException.class)
  public void testWithNullData() throws IOException {
    PlasmaOutputStream pos = new PlasmaOutputStream("testWithEmptyData-dummy-id");
    pos.write(null);
  }

  @Test
  public void testBufferWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(1);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
    pos.write(bytesWrite);

    pos.commitAndGetMetaData();
    assertEquals(1, PlasmaUtils.getNumberOfObjects(blockId));
    assertEquals(PlasmaConf.DEFAULT_BUFFER_SIZE, PlasmaUtils.getSizeOfObjects(blockId));

    byte[] bytesRead = new byte[bytesWrite.length];
    PlasmaInputStream pis = new PlasmaInputStream(blockId);
    pis.read(bytesRead);
    assertArrayEquals(bytesWrite, bytesRead);

    PlasmaUtils.remove(blockId);
    assertFalse(PlasmaUtils.contains(blockId));
  }

  @Test
  public void testPartialBlockWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(2.7);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
    pos.write(bytesWrite);

    pos.commitAndGetMetaData();
    assertEquals(3, PlasmaUtils.getNumberOfObjects(blockId));
    assertEquals(PlasmaConf.DEFAULT_BUFFER_SIZE * 2.7, PlasmaUtils.getSizeOfObjects(blockId), 1);

    ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
    PlasmaInputStream pis = new PlasmaInputStream(blockId);
    byte[] buffer = new byte[PlasmaConf.DEFAULT_BUFFER_SIZE];
    int len;
    while ((len = pis.read(buffer)) != -1) {
      bytesRead.put(buffer, 0, len);
    }
    assertArrayEquals(bytesWrite, bytesRead.array());

    PlasmaUtils.remove(blockId);
    assertFalse(PlasmaUtils.contains(blockId));
  }

  @Test
  public void testMultiBlocksWriteRead() throws IOException {
    String blockId = "block_id_" + random.nextInt(10000000);
    byte[] bytesWrite = prepareByteBlockToWrite(2);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
    pos.write(bytesWrite);

    pos.commitAndGetMetaData();
    assertEquals(2, PlasmaUtils.getNumberOfObjects(blockId));
    assertEquals(PlasmaConf.DEFAULT_BUFFER_SIZE * 2, PlasmaUtils.getSizeOfObjects(blockId));

    ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
    PlasmaInputStream pis = new PlasmaInputStream(blockId);
    byte[] buffer = new byte[PlasmaConf.DEFAULT_BUFFER_SIZE];
    while (pis.read(buffer) != -1) {
      bytesRead.put(buffer);
    }
    assertArrayEquals(bytesWrite, bytesRead.array());

    PlasmaUtils.remove(blockId);
    assertFalse(PlasmaUtils.contains(blockId));
  }

  @Test
  public void testMultiThreadReadWrite() throws InterruptedException {
    int processNum = Runtime.getRuntime().availableProcessors();
    ExecutorService threadPool = Executors.newFixedThreadPool(processNum);
    for (int i = 0; i < 10 * processNum; i++) {
      threadPool.submit(() -> {
        try {
          String blockId = "block_id_" + random.nextInt(10000000);
          byte[] bytesWrite = prepareByteBlockToWrite(5.7);
          PlasmaOutputStream pos = new PlasmaOutputStream(blockId);
          pos.write(bytesWrite);

          pos.commitAndGetMetaData();
          assertEquals(6, PlasmaUtils.getNumberOfObjects(blockId));
          assertEquals(PlasmaConf.DEFAULT_BUFFER_SIZE * 5.7, PlasmaUtils.getSizeOfObjects(blockId), 1);

          ByteBuffer bytesRead = ByteBuffer.allocate(bytesWrite.length);
          PlasmaInputStream pis = new PlasmaInputStream(blockId);
          byte[] buffer = new byte[PlasmaConf.DEFAULT_BUFFER_SIZE];
          int len;
          while ((len = pis.read(buffer)) != -1) {
            bytesRead.put(buffer, 0, len);
          }
          assertArrayEquals(bytesWrite, bytesRead.array());

          PlasmaUtils.remove(blockId);
          assertFalse(PlasmaUtils.contains(blockId));
        } catch (IOException ex) {
          ex.printStackTrace();
        }
      });
    }
    threadPool.shutdown();
    threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
  }

  @Test
  public void testBasicReadWriteWithCompressionAndKryo() {
    testPlasmaObjectWriteRead(true, true);
  }

  @Test
  public void testBasicReadWriteWithoutCompressionAndKryo() {
    testPlasmaObjectWriteRead(false, true);
  }

  @Test
  public void testBasicReadWriteWithCompressionAndJavaSerializer() {
    testPlasmaObjectWriteRead(true, false);
  }

  @Test
  public void testBasicReadWriteWithoutCompressionAndJavaSerializer() {
    testPlasmaObjectWriteRead(false, false);
  }

  private void testPlasmaObjectWriteRead(boolean compressionEnabled, boolean isKyro) {
//    Tuple2<SparkConf, SerializerManager> res = buildConf();
    for (int i = 0; i < 1; i++) {
      BlockId blockId = new ShuffleBlockId(random.nextInt(100), i, random.nextInt(100));
      PlasmaBlockObjectWriter writer = createWriter(blockId, compressionEnabled, isKyro);
      for (int j = 1; j < 10000000; j++) {
        writer.write("key" + j, "value" + j);
      }
      writer.getPartitionLength();

      Iterator<Tuple2<Object, Object>> iterator = createBuf(blockId, compressionEnabled, isKyro).asKeyValueIterator();
      int count = 1;
      while (iterator.hasNext()) {
        Tuple2<Object, Object> next = iterator.next();
        String key = (String) next._1;
        assertEquals("key" + count, key);
        String value = (String) next._2;
        assertEquals("value" + count, value);
        count++;
      }
    }
  }

  @Test
  public void testPlasmaShuffleManagedBuffer() throws IOException {
    ShuffleBlockId blockId = new ShuffleBlockId(99, 99, 99);
    byte[] bytesWrite = prepareByteBlockToWrite(1);
    PlasmaOutputStream pos = new PlasmaOutputStream(blockId.name());
    pos.write(bytesWrite);

    pos.commitAndGetMetaData();
    assertEquals(1, PlasmaUtils.getNumberOfObjects(blockId.name()));
    assertEquals(PlasmaConf.DEFAULT_BUFFER_SIZE, PlasmaUtils.getSizeOfObjects(blockId.name()));

    long size = PlasmaUtils.getSizeOfObjects(blockId.name());
    ManagedBuffer managedBuffer = new PlasmaShuffleManagedBuffer(blockId, size);
    ByteBuffer nioByteBuffer = managedBuffer.nioByteBuffer();

    assertArrayEquals(bytesWrite, nioByteBuffer.array());

    PlasmaUtils.remove(blockId.name());
    assertFalse(PlasmaUtils.contains(blockId.name()));
  }

  @Test
  public void testWriteReadPerf() throws InterruptedException {
    ExecutorService threadPool = Executors.newFixedThreadPool(10);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < 10; i++) {
      int mapId = i;
      threadPool.submit(() -> {
        BlockId blockId = new ShuffleBlockId(0, mapId, 0);
        PlasmaBlockObjectWriter writer = createWriter(blockId, true, true);
        for (int j = 1; j < 3000000; j++) {
          writer.write("key" + j, "value" + j);
        }
        writer.getPartitionLength();

        Iterator<Tuple2<Object, Object>> iterator = createBuf(blockId, true, true).asKeyValueIterator();
        while (iterator.hasNext()) {
          iterator.next();
        }
      });
    }
    threadPool.shutdown();
    threadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.MINUTES);
    long elapsedTime = System.currentTimeMillis() - startTime;
    double bandwidth = (10 * 81837767 / 1024 / 1024) / (elapsedTime / 1000);
    System.out.println(bandwidth + "MB/s");
  }

  @AfterClass
  public static void tearDown() {
    try {
      MyPlasmaClientHolder.close();
      PlasmaStoreServer.stopPlasmaStore();
      PlasmaStoreServer.deletePlasmaSocketFile();
    } catch (InterruptedException ex) {
      ex.printStackTrace();
    }
  }

  private byte[] prepareByteBlockToWrite(double numOfBlock) {
    byte[] bytesToWrite = new byte[(int) (PlasmaConf.DEFAULT_BUFFER_SIZE * numOfBlock)];
    random.nextBytes(bytesToWrite);
    return bytesToWrite;
  }

  private static void mockSparkEnv() {
    SparkConf conf = new SparkConf();
    conf.set("spark.io.plasma.server.socket", "/tmp/PlasmaOutputInputStreamSuite-Socket-File");
    conf.set("spark.io.plasma.server.dir", PlasmaConf.DEFAULT_STORE_SERVER_DIR_VALUE);
    conf.set("spark.io.plasma.server.memory", PlasmaConf.DEFAULT_STORE_SERVER_MEMORY_VALUE);

    SparkEnv mockEnv = mock(SparkEnv.class);
    SparkEnv.set(mockEnv);
    when(mockEnv.conf()).thenReturn(conf);
  }

  private PlasmaBlockObjectWriter createWriter(BlockId blockId, boolean compressionEnabled, boolean isKryo) {
    SparkConf conf = new SparkConf();
    conf.set("spark.shuffle.compress", String.valueOf(compressionEnabled));
    Serializer serializer = isKryo? new KryoSerializer(conf): new JavaSerializer(conf);

    PlasmaBlockObjectWriter objectWriter = new PlasmaBlockObjectWriter(
            new SerializerManager(serializer, conf),
            serializer.newInstance(),
            blockId);
    return objectWriter;
  }

  private DeserializationStream createBuf(BlockId blockId, boolean compressionEnabled, boolean isKryo) {
    PlasmaInputStream pis = new PlasmaInputStream(blockId.name());
    SparkConf conf = new SparkConf();
    conf.set("spark.shuffle.compress", String.valueOf(compressionEnabled));

    Serializer serializer = isKryo? new KryoSerializer(conf): new JavaSerializer(conf);
    SerializerManager serializerManager = new SerializerManager(serializer, conf);
    InputStream input = serializerManager.wrapStream(blockId, pis);

    return serializer.newInstance().deserializeStream(input);
  }
}
