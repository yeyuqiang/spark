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

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}

object PlasmaShuffleConf {

  val STORE_SERVER_SOCKET_KEY: ConfigEntry[String] =
    ConfigBuilder("spark.io.plasma.server.socket")
      .doc("Specifies the socket that the plasma store will listen at")
      .stringConf
      .createWithDefault("/tmp/plasma")

  val STORE_SERVER_DIR_KEY: ConfigEntry[String] =
    ConfigBuilder("spark.io.plasma.server.dir")
      .doc("Specifies the mount point of the huge page file system")
      .stringConf
      .createWithDefault("/dev/shm")

  val STORE_SERVER_MEMORY_KEY: ConfigEntry[String] =
    ConfigBuilder("spark.io.plasma.server.memory")
      .doc("Specifies the size of the store in bytes")
      .stringConf
      .createWithDefault("1000000000")

  val STORE_SERVER_AUTO_START: ConfigEntry[Boolean] =
    ConfigBuilder("spark.io.plasma.auto.start")
      .doc("Specifies whether plasma store will start automatically")
      .booleanConf
      .createWithDefault(false)

}
