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

import org.apache.commons.lang3.StringUtils;

public class PlasmaObjectId {

  private final String objectId;

  public PlasmaObjectId(String parentObjectId, int index) {
    int parentObjectIdLen = parentObjectId.length();
    int indexDigitNum = String.valueOf(index).length();

    if (parentObjectIdLen + indexDigitNum > 20) {
      throw new IllegalArgumentException("Each object in the Plasma store" +
          " should be associated with a unique ID which is a string of 20 length");
    } else {
      this.objectId = StringUtils.rightPad(parentObjectId, 20 - indexDigitNum, "0") + index;
    }
  }

  public byte[] toBytes() {
    return objectId.getBytes();
  }
}