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
import org.apache.spark.SparkEnv;

import java.nio.ByteBuffer;

/**
 * Upstream Plasma Client Wrapper.
 * To simplify the parameter passed to plasma client.
 * Make finalize() in PlasmaClient can be invoked explicitly.
 * This class should be remove in the future.
 */
public class MyPlasmaClient extends PlasmaClient {

  public MyPlasmaClient(String storeSocketName) {
    super(storeSocketName, "", 0);
  }

  public void writeChunk(String parentObjectId, int index, byte[] buffer) {
    PlasmaObjectId objectId = new PlasmaObjectId(parentObjectId, index);
    put(objectId.toBytes(), buffer, null);
  }

  public ByteBuffer readChunk(String parentObjectId, int index) {
    PlasmaObjectId childObjectId = new PlasmaObjectId(parentObjectId, index);
    ByteBuffer buffer = getObjAsByteBuffer(childObjectId.toBytes(), 0, false);
    if (buffer != null) {
      release(childObjectId.toBytes());
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

  public static MyPlasmaClient get() {
    if (client == null) {
      String storeSocketName = SparkEnv.get() == null ?
          PlasmaConf.DEFAULT_STORE_SERVER_SOCKET_VALUE :
          SparkEnv.get().conf().get(PlasmaConf.STORE_SERVER_SOCKET_KEY);
      client = new MyPlasmaClient(storeSocketName);
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
