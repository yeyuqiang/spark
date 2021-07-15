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

import org.apache.spark.{ShuffleDependency, SparkConf, SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.shuffle._

private[spark] class PlasmaShuffleManager(conf: SparkConf)
  extends ShuffleManager with Logging {

  startPlasmaStore
  initPlasmaClient

  lazy val autoStart = conf.get(PlasmaShuffleConf.STORE_SERVER_AUTO_START)
  lazy val plasmaStoreSocket = conf.get(PlasmaShuffleConf.STORE_SERVER_SOCKET_KEY)
  lazy val plasmaStoreMemory = conf.get(PlasmaShuffleConf.STORE_SERVER_MEMORY_KEY)
  lazy val plasmaStoreDir = conf.get(PlasmaShuffleConf.STORE_SERVER_DIR_KEY)

  private[this] def startPlasmaStore(): Unit = {
    if (!autoStart) {
      return
    }
    PlasmaStoreServer.startPlasmaStoreWithLock(plasmaStoreSocket, plasmaStoreMemory, plasmaStoreDir)
  }

  private[this] def initPlasmaClient(): Unit = {
    if (!autoStart) {
      logInfo("Please start plasma store manually before initialize plasma client.")
    }
    MyPlasmaClientHolder.init(plasmaStoreSocket)
  }

  override val shuffleBlockResolver = new PlasmaShuffleBlockResolver()

  override def registerShuffle[K, V, C](
      shuffleId: Int,
      dependency: ShuffleDependency[K, V, C]): ShuffleHandle = {
    new PlasmaShuffleHandle(shuffleId, dependency)
  }

  override def getWriter[K, V](
      handle: ShuffleHandle,
      mapId: Long, context: TaskContext,
      metrics: ShuffleWriteMetricsReporter): ShuffleWriter[K, V] = {
    val env = SparkEnv.get
    val serializerManager = env.serializerManager

    handle match {
      case plasmaShuffleHandle: PlasmaShuffleHandle[K @unchecked, V @unchecked, _] =>
        new PlasmaShuffleWriter(
          env.blockManager,
          plasmaShuffleHandle,
          mapId,
          serializerManager)
    }
  }

  override def getReader[K, C](
      handle: ShuffleHandle,
      startMapIndex: Int,
      endMapIndex: Int,
      startPartition: Int,
      endPartition: Int,
      context: TaskContext,
      metrics: ShuffleReadMetricsReporter): ShuffleReader[K, C] = {
    val blockByAddress = SparkEnv.get.mapOutputTracker.getMapSizesByExecutorId(
      handle.shuffleId, startMapIndex, endMapIndex, startPartition, endPartition
    )
    new PlasmaShuffleReader(
      handle.asInstanceOf[PlasmaShuffleHandle[K, _, C]],
      blockByAddress,
      context)
  }

  override def unregisterShuffle(shuffleId: Int): Boolean = {
    shuffleBlockResolver.removeDataByMap()
    true
  }

  override def stop(): Unit = {
    if (autoStart) {
      shuffleBlockResolver.stop()
      PlasmaStoreLockFile.unlock()
      PlasmaStoreServer.deletePlasmaSocketFile(plasmaStoreSocket)
    }
  }
}

/**
 * Plasma ShuffleHandle implementation that just captures registerShuffle's parameters.
 */
private[spark] class PlasmaShuffleHandle[K, V, C](
    shuffleId: Int,
    val dependency: ShuffleDependency[K, V, C])
  extends ShuffleHandle(shuffleId)

