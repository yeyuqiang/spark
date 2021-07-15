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

import org.apache.spark.{Partitioner, ShuffleDependency}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric._
import org.apache.spark.sql.types._

object UnsafeRowWithPlasma {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Simple Partition Example")
      .config("spark.shuffle.manager", "org.apache.spark.shuffle.pmem.PlasmaShuffleManager")
      .config("spark.io.plasma.server.socket", "/tmp/plasma")
      .config("spark.io.plasma.server.dir", "/dev/shm")
      .config("spark.io.plasma.server.memory", "500000000")
      .config("spark.io.plasma.auto.start", true)
      .master("local")
      .getOrCreate()

    val row = Row("Hello", 123)
    val unsafeRow = toUnsafeRow(row, Array(StringType, IntegerType))
    val rowsRDD = spark.sparkContext.parallelize(
      Seq((0, unsafeRow), (1, unsafeRow), (0, unsafeRow))
    ).asInstanceOf[RDD[Product2[Int, InternalRow]]]
    val dependency =
      new ShuffleDependency[Int, InternalRow, InternalRow](
        rowsRDD,
        new PartitionIdPassthrough(2),
        new UnsafeRowSerializer(2))
    val shuffled = new ShuffledRowRDD(
      dependency,
      SQLShuffleReadMetricsReporter.createShuffleReadMetrics(spark.sparkContext))
    val count = shuffled.count()
    // scalastyle:off println
    println(count)
    // scalastyle:on println
  }

  private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
    override def getPartition(key: Any): Int = key.asInstanceOf[Int]
  }

  private def toUnsafeRow(row: Row, schema: Array[DataType]): UnsafeRow = {
    val converter = unsafeRowConverter(schema)
    converter(row)
  }

  private def unsafeRowConverter(schema: Array[DataType]): Row => UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    (row: Row) => {
      converter(CatalystTypeConverters.convertToCatalyst(row).asInstanceOf[InternalRow])
    }
  }

}
