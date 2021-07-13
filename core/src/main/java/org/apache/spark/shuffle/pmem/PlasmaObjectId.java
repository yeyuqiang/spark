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

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

public class PlasmaObjectId {

  private final String objectId;
  private static HashFunction hf = Hashing.murmur3_128();

  public PlasmaObjectId(String parentObjectId, int index) {
    this.objectId = parentObjectId + index;
  }

  public byte[] toBytes() {
    return hash(objectId);
  }

  private byte[] hash(String objectId) {
    byte[] ret = new byte[20];
    hf.newHasher().putBytes(objectId.getBytes()).hash().writeBytesTo(ret, 0, 20);
    return ret;
  }

  @Override
  public String toString() {
    return objectId;
  }
}