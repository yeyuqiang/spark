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
package org.apache.spark.examples

// import org.apache.log4j.{Level, Logger}

import org.apache.spark.HashPartitioner
import org.apache.spark.sql.SparkSession

object SimplePartitionExample {
  def main(args: Array[String]): Unit = {
    // scalastyle:off println
//    Logger.getLogger("org").setLevel(Level.OFF)
    val spark = SparkSession
      .builder
      .appName("Simple Partition Example")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.pmem.PlasmaShuffleManager")
      .config("spark.io.plasma.server.socket", "/tmp/plasma")
      .config("spark.io.plasma.server.dir", "/dev/shm")
      .config("spark.io.plasma.server.memory", "1000000000")
      .config("spark.shuffle.compress", "false")
      .master("local")
      .getOrCreate()
    val sc = spark.sparkContext

    val data1 = Array[(Int, Char)](
      (1, 'a'), (2, 'b'),
      (3, 'c'), (4, 'd'),
      (5, 'e'), (3, 'f'),
      (2, 'g'), (1, 'h'))
    val rdd1 = sc.parallelize(data1, 3)
    val partitionedRDD = rdd1.partitionBy(new HashPartitioner(3))

    println("----- partitionedRDD -----")
    partitionedRDD.mapPartitionsWithIndex((pid, iter) => {
      iter.map(value => "PID: " + pid + ", value: " + value)
    }).foreach(println)
//    System.in.read()
  }

}
