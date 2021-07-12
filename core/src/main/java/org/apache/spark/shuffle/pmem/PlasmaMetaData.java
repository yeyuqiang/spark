/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.pmem;

import java.nio.ByteBuffer;

/**
 * Use 8 bytes to record plasma object metadata.
 * The first 4 bytes will record total number of objects.
 * The last 4 bytes will record total size of object.
 */
public class PlasmaMetaData {
  ByteBuffer buf;

  public PlasmaMetaData(int num, long len) {
    buf = ByteBuffer.allocate(12);
    buf.putInt(num);
    buf.putLong(len);
  }

  public PlasmaMetaData(ByteBuffer buf) {
    this.buf = buf;
  }

  public int getTotalNumber() {
    return buf.getInt(0);
  }

  public long getTotalSize() {
    return buf.getLong(4);
  }

  public byte[] getBytes() {
    return buf.array();
  }
}
