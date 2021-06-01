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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.shuffle.ShuffleBlockResolver
import org.apache.spark.storage._

private[spark] class PlasmaShuffleBlockResolver(
  var _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  val blockIds = new ArrayBuffer[String]()

  override def getBlockData(
      blockId: BlockId,
      dirs: Option[Array[String]]): ManagedBuffer = {
    blockIds += blockId.name
    val length = PlasmaUtils.getSizeOfObjects(blockId.name)
    new PlasmaInputManagedBuffer(blockId, length)
  }

  override def stop(): Unit = {
    // Stop plasma store server for easy test
    PlasmaStoreServer.stopPlasmaStore()
  }

  def removeDataByMap(): Unit = {
    blockIds.toArray.foreach(blockId => {
      PlasmaUtils.remove(blockId)
    })
  }
}
