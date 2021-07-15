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

package org.apache.spark.shuffle.pmem

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.ShuffleWriter
import org.apache.spark.storage.{BlockManager, ShuffleBlockId}

private[spark] class PlasmaShuffleWriter[K, V, C](
    blockManager: BlockManager,
    handle: PlasmaShuffleHandle[K, V, C],
    mapId: Long,
    serializerManager: SerializerManager)
  extends ShuffleWriter[K, V] with Logging  {

  private val dep = handle.dependency
  private val serializer = dep.serializer;

  private val partitioner = dep.partitioner
  private val numPartitions = partitioner.numPartitions
  private val shuffleId = dep.shuffleId

  private val partitionLengths = new Array[Long](numPartitions)
  private var mapStatus: MapStatus = null

  private var stopping = false

  /** Write a sequence of records to this task's output */
  override def write(records: Iterator[Product2[K, V]]): Unit = {
    val plasmaOutputStreamArray = (0 until numPartitions).toArray.map(partitionId =>
      new PlasmaBlockObjectWriter(
        serializerManager,
        serializer.newInstance(),
        ShuffleBlockId(shuffleId, mapId, partitionId)
      )
    )

    while (records.hasNext) {
      val record = records.next()
      val key = record._1
      val value = record._2
      val partitionNum = partitioner.getPartition(key)
      plasmaOutputStreamArray(partitionNum).write(key, value)
    }

    (0 until numPartitions).toArray.map(partitionId =>
      partitionLengths(partitionId) = plasmaOutputStreamArray(partitionId).getPartitionLength()
    )

    mapStatus = MapStatus(blockManager.shuffleServerId, partitionLengths, mapId)
  }

  /** Close this writer, passing along whether the map completed */
  override def stop(success: Boolean): Option[MapStatus] = {
    if (stopping) {
      return None
    }
    stopping = true
    if (success) {
      Option(mapStatus)
    } else {
      None
    }
  }

}
