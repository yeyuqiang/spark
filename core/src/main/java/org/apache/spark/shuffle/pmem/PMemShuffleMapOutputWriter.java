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
import org.apache.spark.internal.config.package$;
import org.apache.spark.shuffle.IndexShuffleBlockResolver;
import org.apache.spark.shuffle.api.ShuffleMapOutputWriter;
import org.apache.spark.shuffle.api.ShufflePartitionWriter;
import org.apache.spark.shuffle.api.WritableByteChannelWrapper;
import org.apache.spark.shuffle.api.metadata.MapOutputCommitMessage;
import org.apache.spark.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Optional;

/**
 * Implementation of {@link ShuffleMapOutputWriter} that replicates the functionality of shuffle
 * persisting shuffle data to local disk alongside index files, identical to Spark's historic
 * canonical shuffle storage mechanism.
 */
public class PMemShuffleMapOutputWriter implements ShuffleMapOutputWriter {

  private static final Logger log =
    LoggerFactory.getLogger(PMemShuffleMapOutputWriter.class);

  private final int shuffleId;
  private final long mapId;
  private final PMemShuffleBlockResolver blockResolver;
  private final long[] partitionLengths;
  private final int bufferSize;
  private int lastPartitionId = -1;

  private BufferedOutputStream outputBufferedFileStream;

  public PMemShuffleMapOutputWriter(
      int shuffleId,
      long mapId,
      int numPartitions,
      PMemShuffleBlockResolver blockResolver,
      SparkConf sparkConf) {
    this.shuffleId = shuffleId;
    this.mapId = mapId;
    this.blockResolver = blockResolver;
    this.bufferSize =
      (int) (long) sparkConf.get(
        package$.MODULE$.SHUFFLE_UNSAFE_FILE_OUTPUT_BUFFER_SIZE()) * 1024;
    this.partitionLengths = new long[numPartitions];
  }

  @Override
  public ShufflePartitionWriter getPartitionWriter(int reducePartitionId) throws IOException {
    if (reducePartitionId <= lastPartitionId) {
      throw new IllegalArgumentException("Partitions should be requested in increasing order.");
    }
    lastPartitionId = reducePartitionId;
    return new PMemShufflePartitionWriter(reducePartitionId);
  }

  @Override
  public MapOutputCommitMessage commitAllPartitions() throws IOException {
    log.info("PMemShuffleMapOutputWriter commitAllPartitions");
    return MapOutputCommitMessage.of(partitionLengths);
  }

  @Override
  public void abort(Throwable error) throws IOException {

  }


  private class PMemShufflePartitionWriter implements ShufflePartitionWriter {

    private final int partitionId;


    private PMemShufflePartitionWriter(int partitionId) {
      this.partitionId = partitionId;
    }

    @Override
    public OutputStream openStream() throws IOException {
      return null;
    }

    @Override
    public Optional<WritableByteChannelWrapper> openChannelWrapper() throws IOException {
      return Optional.of(null);
    }

    @Override
    public long getNumBytesWritten() {
      return 0;
    }
  }

}
