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

import org.apache.arrow.plasma.PlasmaClient;
import org.apache.arrow.plasma.exceptions.DuplicateObjectException;
import org.apache.arrow.plasma.exceptions.PlasmaClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Upstream Plasma Client Wrapper.
 * To simplify the parameter passed to plasma client.
 * Make finalize() in PlasmaClient can be invoked explicitly.
 * This class should be remove in the future.
 */
public class MyPlasmaClient extends PlasmaClient {

  private static final Logger logger = LoggerFactory.getLogger(MyPlasmaClient.class);

  public MyPlasmaClient(String storeSocketName) {
    super(storeSocketName, "", 0);
  }

  public void writeChunk(String parentObjectId, int index, byte[] buffer) {
    PlasmaObjectId objectId = new PlasmaObjectId(parentObjectId, index);
    try {
      put(objectId.toBytes(), buffer, null);
    } catch (DuplicateObjectException ex) {
      logger.warn("Skipping write this plasma object due to duplicated");
    }
  }

  public ByteBuffer readChunk(String parentObjectId, int index) {
    PlasmaObjectId objectId = new PlasmaObjectId(parentObjectId, index);
    ByteBuffer buffer = getObjAsByteBuffer(objectId.toBytes(), 0, false);
    if (buffer != null) {
      release(objectId.toBytes());
    }
    return buffer;
  }

  @Override
  public void finalize() {
    super.finalize();
  }
}

/**
 * Hold a global plasma client instance.
 */
class MyPlasmaClientHolder {

  private static MyPlasmaClient client;

  static {
    System.loadLibrary("plasma_java");
  }

  private MyPlasmaClientHolder() {
  }

  public static MyPlasmaClient init(String plasmaStoreSocket) {
    if (client == null) {
      client = new MyPlasmaClient(plasmaStoreSocket);
    }
    return client;
  }

  public static MyPlasmaClient get() {
    if (client == null) {
      throw new PlasmaClientException("Plasma client is not initialized.");
    }
    return client;
  }

  public static void close() {
    if (client != null) {
      client.finalize();
      client = null;
    }
  }
}
