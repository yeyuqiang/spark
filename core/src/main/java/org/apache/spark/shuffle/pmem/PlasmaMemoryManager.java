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

package org.apache.spark.shuffle.pmem;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicLong;

/**
 * PMem Memory Manager
 * Handle whether the system can provide enough pmem memory in numeric
 */
public class PlasmaMemoryManager {

  private long totalMemory;
  @VisibleForTesting
  AtomicLong memoryUsed = new AtomicLong(0);

  private double useRatio;

  public PlasmaMemoryManager(long totalMemory, double useRatio) {
    this.totalMemory = totalMemory;
    this.useRatio = useRatio;
  }

  // Make pmem can be written when usage is lower than use ratio
  public boolean isAvailable() {
    return (totalMemory * useRatio - memoryUsed.get()) > 0;
  }

  public void increase(long memorySize) {
    memoryUsed.getAndAdd(memorySize);
  }

  public void release(long memorySize) {
    memoryUsed.getAndAdd(memorySize * -1);
  }

}
