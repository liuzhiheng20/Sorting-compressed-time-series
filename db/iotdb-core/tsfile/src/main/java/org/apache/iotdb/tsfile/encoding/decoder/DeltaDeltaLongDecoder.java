package org.apache.iotdb.tsfile.encoding.decoder;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_64BIT;

public class DeltaDeltaLongDecoder extends Decoder {
    protected boolean firstValueWasRead = false;
    private byte buffer = 0;   // 写入缓冲区
    protected int bitsLeft = Byte.SIZE;   // 写入数量指标
    protected long lastDelta = 0;
    protected long lastValue = 0;

    protected long newDeltaDelta = 0;  // 存储下一个二阶delta，从而判断有没有后面的值

    public DeltaDeltaLongDecoder() {
        super(TSEncoding.DELTA_OF_DELTA);
    }

    @Override
    public void reset() {
        firstValueWasRead = false;
        lastDelta = 0;
        lastValue = 0;
        newDeltaDelta = 0;
        buffer = 0;
        bitsLeft = 0;
    }

    /**
     * Reads the next bit and returns a boolean representing it.
     *
     * @return true if the next bit is 1, otherwise 0.
     */
    protected boolean readBit(ByteBuffer in) {
        boolean bit = ((buffer >> (bitsLeft - 1)) & 1) == 1;
        bitsLeft--;
        flipByte(in);
        return bit;
    }

    /**
     * Reads a long from the next X bits that represent the least significant bits in the long value.
     *
     * @param bits How many next bits are read from the stream
     * @return long value that was read from the stream
     */

    protected long readLong(int bits, ByteBuffer in) {
        long value = 0;
        while (bits > 0) {
            if (bits > bitsLeft || bits == Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                byte d = (byte) (buffer & ((1 << bitsLeft) - 1));
                value = (value << bitsLeft) + (d & 0xFF);
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                // Shift to correct position and take only least significant bits
                byte d = (byte) ((buffer >>> (bitsLeft - bits)) & ((1 << bits) - 1));
                value = (value << bits) + (d & 0xFF);
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte(in);
        }
        return value;
    }

    protected void flipByte(ByteBuffer in) {
        if (bitsLeft == 0) {
            buffer = in.get();
            bitsLeft = Byte.SIZE;
        }
    }

    @Override
    public final boolean hasNext(ByteBuffer in) {
        if (!firstValueWasRead){
            return true;
        }
        long deltaDelta = 0;
        if(!readBit(in)){
            deltaDelta = 0;
            lastDelta = lastDelta+deltaDelta;
            return true;
        }
        if(!readBit(in)) {
            deltaDelta = readLong(7, in);
            if(deltaDelta>64){
                deltaDelta = deltaDelta-128;
            }
            lastDelta = lastDelta+deltaDelta;
            return true;
        }
        if(!readBit(in)) {
            deltaDelta = readLong(9, in);
            if(deltaDelta>256){
                deltaDelta = deltaDelta-512;
            }
            lastDelta = lastDelta+deltaDelta;
            return true;
        }
        if(!readBit(in)) {
            deltaDelta = readLong(12, in);
            if(deltaDelta>2048){
                deltaDelta = deltaDelta-4096;
            }
            lastDelta = lastDelta+deltaDelta;
            return true;
        } else {
            deltaDelta = readLong(32, in);
            if(deltaDelta>Integer.MAX_VALUE){
                deltaDelta = deltaDelta-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
            }
            if(deltaDelta==GORILLA_ENCODING_ENDING_INTEGER) {  //todo:这里用不用区分long类型和interger类型？
                return false;
            }
            lastDelta = lastDelta+deltaDelta;
            return true;
        }
    }

    @Override
    public final long readLong(ByteBuffer in) {  // 最终对外的接口
        if(!firstValueWasRead){
            lastValue = readLong(VALUE_BITS_LENGTH_64BIT, in);
            firstValueWasRead = true;
        } else{
            lastValue = lastValue+lastDelta;
        }
        return lastValue;
    }

    public int getBitsLeft() {
        return bitsLeft;
    }

}

