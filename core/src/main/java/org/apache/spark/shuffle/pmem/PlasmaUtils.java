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
 * Utility to operate object stored in plasma server
 */
public class PlasmaUtils {

  public final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

  private static MyPlasmaClient client = MyPlasmaClientHolder.get();

  public static boolean contains(String blockId) {
    int num = getNumberOfObjects(blockId);
    return num > 0;
  }

  public static void remove(String blockId) {
    int num = getNumberOfObjects(blockId);
    for (int i = 0; i < num; i++) {
      PlasmaObjectId childObjectId = new PlasmaObjectId(blockId, i);
      client.delete(childObjectId.toBytes());
    }
    PlasmaObjectId metaDataId = new PlasmaObjectId(blockId, -1);
    client.delete(metaDataId.toBytes());
  }

  public static int getNumberOfObjects(String blockId) {
    ByteBuffer buffer = client.readChunk(blockId, -1);
    if (buffer == null) return -1;

    PlasmaMetaData metaData = new PlasmaMetaData(buffer);
    return metaData.getTotalNumber();
  }

  public static long getSizeOfObjects(String blockId) {
    ByteBuffer buffer = client.readChunk(blockId, -1);
    if (buffer == null) return -1;

    PlasmaMetaData metaData = new PlasmaMetaData(buffer);
    return metaData.getTotalSize();
  }
}
