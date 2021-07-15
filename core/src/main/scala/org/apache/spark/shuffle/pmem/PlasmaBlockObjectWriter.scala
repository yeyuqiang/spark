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

import java.io.{BufferedOutputStream, OutputStream}

import org.apache.spark.internal.Logging
import org.apache.spark.serializer._
import org.apache.spark.storage.BlockId
import org.apache.spark.util.Utils
import org.apache.spark.util.collection.PairsWriter

/**
 * A class for writing JVM objects directly to Plasma.
 */
private[spark] class PlasmaBlockObjectWriter(
    serializerManager: SerializerManager,
    serializerInstance: SerializerInstance,
    blockId: BlockId)
  extends OutputStream
  with Logging
  with PairsWriter {

  private trait ManualCloseOutputStream extends OutputStream {
    abstract override def close(): Unit = {
      flush()
    }

    def manualClose(): Unit = {
      super.close()
    }
  }

  private var mcs: ManualCloseOutputStream = null
  private var bs: OutputStream = _
  private var pos: PlasmaOutputStream = _
  private var objOut: SerializationStream = _
  private var initialized = false
  private var streamOpen = false
  private var hasBeenClosed = false

  private def initialize(): Unit = {
    pos = new PlasmaOutputStream(blockId.name)
    class ManualCloseBufferedOutputStream
      extends BufferedOutputStream(pos, PlasmaUtils.DEFAULT_BUFFER_SIZE)
        with ManualCloseOutputStream
    mcs = new ManualCloseBufferedOutputStream
  }

  def open(): PlasmaBlockObjectWriter = {
    if (hasBeenClosed) {
      throw new IllegalStateException("Writer already closed. Cannot be reopened.")
    }

    if (!initialized) {
      initialize()
      initialized = true
    }
    bs = serializerManager.wrapStream(blockId, mcs)
    objOut = serializerInstance.serializeStream(bs)
    streamOpen = true
    this
  }

  override def close(): Unit = {
    if (initialized) {
      Utils.tryWithSafeFinally {
        mcs.manualClose()
      } {
        mcs = null
        bs = null
        pos = null
        objOut = null
        initialized = false
        streamOpen = false
        hasBeenClosed = true
      }
    }
  }

  override def write(b: Int): Unit = {
    throw new UnsupportedOperationException()
  }

  override def write(key: Any, value: Any): Unit = {
    if (!streamOpen) {
      open()
    }
    objOut.writeKey(key)
    objOut.writeValue(value)
  }

  override def write(kvBytes: Array[Byte], offs: Int, len: Int): Unit = {
    if (!streamOpen) {
      open()
    }

    bs.write(kvBytes, offs, len)
  }

  def getPartitionLength(): Long = {
    if (!streamOpen) {
      0
    } else {
      bs.flush()
      pos.commitAndGetMetaData().getTotalSize
    }
  }

}


