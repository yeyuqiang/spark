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

import java.io.InputStream
import java.nio.ByteBuffer

import io.netty.buffer.Unpooled

import org.apache.spark.network.buffer.ManagedBuffer
import org.apache.spark.storage.BlockId

private[spark] class PlasmaShuffleManagedBuffer(
    blockId: BlockId,
    size: Long)
  extends ManagedBuffer {

  override def size(): Long = {
    size
  }

  override def createInputStream(): InputStream = new PlasmaInputStream(blockId.name)

  override def retain(): ManagedBuffer = this

  override def release(): ManagedBuffer = this

  override def nioByteBuffer(): ByteBuffer = {
    val in = createInputStream()
    val readBuf = ByteBuffer.allocate(size().toInt)
    val buf = new Array[Byte](PlasmaUtils.DEFAULT_BUFFER_SIZE)
    var len = 0
    while ( {
      len = in.read(buf);
      len
    } != -1) {
      readBuf.put(buf, 0, len)
    }
    readBuf.flip()
    readBuf
  }

  override def convertToNetty(): AnyRef = {
    val buf = nioByteBuffer()
    Unpooled.wrappedBuffer(buf)
  }
}
