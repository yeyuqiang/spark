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
package org.apache.spark.examples.sql

// $example on:init_session$
import org.apache.spark.sql.SparkSession
// $example off:init_session$

object SparkSQLWithGroupBy {

  // $example on:create_ds$
  case class Person(name: String, age: Long)
  // $example off:create_ds$

  def main(args: Array[String]): Unit = {
    // $example on:init_session$
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.pmem.PlasmaShuffleManager")
      .config("spark.io.plasma.server.socket", "/tmp/plasma")
      .config("spark.io.plasma.server.dir", "/dev/shm")
      .config("spark.io.plasma.server.memory", "1000000000")
      .config("spark.io.plasma.auto.start", true)
      .config("spark.shuffle.compress", "false")
      .master("local")
      .getOrCreate()

    runBasicDataFrameExample(spark)

    spark.stop()
  }

  private def runBasicDataFrameExample(spark: SparkSession): Unit = {
    // $example on:create_df$
    val df = spark.read.json("examples/src/main/resources/people.json")
    df.createOrReplaceTempView("people")
    val sqlDF = spark.sql("SELECT SUM(age) FROM people GROUP BY age")
    sqlDF.show()
  }

}
