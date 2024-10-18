/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding.decoder;

import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.*;

/**
 * This class includes code modified from Michael Burman's gorilla-tsc project.
 *
 * <p>Copyright: 2016-2018 Michael Burman and/or other contributors
 *
 * <p>Project page: https://github.com/burmanm/gorilla-tsc
 *
 * <p>License: http://www.apache.org/licenses/LICENSE-2.0
 */
public class LongGorillaDecoder extends GorillaDecoderV2 {

  protected long storedValue = 0;

  protected long[] storedValues = new long[] {0, 0, 0}; // 用来提供加速读取

  @Override
  public void reset() {
    super.reset();
    storedValue = 0;
  }

  @Override
  public final long readLong(ByteBuffer in) {
    long returnValue = storedValue;
    if (!firstValueWasRead) {
      flipByte(in);
      storedValue = readLong(VALUE_BITS_LENGTH_64BIT, in);
      firstValueWasRead = true;
      returnValue = storedValue;
    }
    cacheNext(in);
    return returnValue;
  }

  public final long[] readLong2(ByteBuffer in) {
    long[] returnValues = storedValues;
    if (!firstValueWasRead) {
      flipByte(in);
      storedValue = readLong(VALUE_BITS_LENGTH_64BIT, in);
      storedValues = new long[]{64,storedValue,storedValue};
      firstValueWasRead = true;
      returnValues = storedValues;
    }
    cacheNext2(in);
    return returnValues;
  }

  protected long cacheNext(ByteBuffer in) {
    readNext(in);
    if (storedValue == GORILLA_ENCODING_ENDING_LONG) {
      hasNext = false;
    }
    return storedValue;
  }

  protected long[] cacheNext2(ByteBuffer in) {
    readNext2(in);
    if (storedValue == Double.doubleToRawLongBits(GORILLA_ENCODING_ENDING_DOUBLE)) {
      hasNext = false;
    }
    return storedValues;
  }

  @SuppressWarnings("squid:S128")
  protected long readNext(ByteBuffer in) {
    byte controlBits = readNextClearBit(2, in);

    switch (controlBits) {
      case 3: // case '11': use new leading and trailing zeros
        storedLeadingZeros = (int) readLong(LEADING_ZERO_BITS_LENGTH_64BIT, in);
        byte significantBits = (byte) readLong(MEANINGFUL_XOR_BITS_LENGTH_64BIT, in);
        significantBits++;
        storedTrailingZeros = VALUE_BITS_LENGTH_64BIT - significantBits - storedLeadingZeros;
        // missing break is intentional, we want to overflow to next one
      case 2: // case '10': use stored leading and trailing zeros
        long xor = readLong(VALUE_BITS_LENGTH_64BIT - storedLeadingZeros - storedTrailingZeros, in);
        xor <<= storedTrailingZeros;
        storedValue ^= xor;
        // missing break is intentional, we want to overflow to next one
      default: // case '0': use stored value
        return storedValue;
    }
  }

  protected void readNextClearBit2(int maxBits, ByteBuffer in) {
    storedValues[0] = 0;
    storedValues[1] = 0;
    storedValues[2] = 0;

    for (int i = 0; i < maxBits; i++) {
      storedValues[0]++;
      storedValues[1]<<= 1;
      if (readBit(in)) {
        storedValues[1]|= 0x01;
      } else {
        break;
      }
    }
  }

  protected long[] readNext2(ByteBuffer in) {
    // 开始读取数据
    readNextClearBit2(2, in);

    switch ((int)storedValues[1]) {
      case 3: // case '11': use new leading and trailing zeros
        storedLeadingZeros = (int) readLong(LEADING_ZERO_BITS_LENGTH_64BIT, in);
        storedValues[1] <<= LEADING_ZERO_BITS_LENGTH_64BIT;
        storedValues[1] |= storedLeadingZeros;
        storedValues[0] += LEADING_ZERO_BITS_LENGTH_64BIT;
        byte significantBits = (byte) readLong(MEANINGFUL_XOR_BITS_LENGTH_64BIT, in);
        storedValues[1] <<=  MEANINGFUL_XOR_BITS_LENGTH_64BIT;
        storedValues[1] |= significantBits;
        storedValues[0] += MEANINGFUL_XOR_BITS_LENGTH_64BIT;
        significantBits++;
        storedTrailingZeros = VALUE_BITS_LENGTH_64BIT - significantBits - storedLeadingZeros;
        // missing break is intentional, we want to overflow to next one
      case 2: // case '10': use stored leading and trailing zeros
        long xor = readLong(VALUE_BITS_LENGTH_64BIT - storedLeadingZeros - storedTrailingZeros, in);
        storedValues[1] <<=  VALUE_BITS_LENGTH_64BIT - storedLeadingZeros - storedTrailingZeros;
        storedValues[1] |= xor;
        storedValues[0] += VALUE_BITS_LENGTH_64BIT - storedLeadingZeros - storedTrailingZeros;
        xor <<= storedTrailingZeros;
        storedValue ^= xor;
        // missing break is intentional, we want to overflow to next one
      default: // case '0': use stored value
        return storedValues;
    }
  }
}
