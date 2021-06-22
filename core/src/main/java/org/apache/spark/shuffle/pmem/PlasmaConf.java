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

public class PlasmaConf {

  public final static int DEFAULT_BUFFER_SIZE = 1 * 1024 * 1024;

  public final static String STORE_SERVER_SOCKET_KEY = "spark.io.plasma.server.socket";
  public final static String DEFAULT_STORE_SERVER_SOCKET_VALUE = "/tmp/plasma";

  public final static String STORE_SERVER_DIR_KEY = "spark.io.plasma.server.dir";
  public final static String DEFAULT_STORE_SERVER_DIR_VALUE = "/dev/shm";

  public final static String STORE_SERVER_MEMORY_KEY = "spark.io.plasma.server.memory";
  public final static String DEFAULT_STORE_SERVER_MEMORY_VALUE = "1000000000";


}
