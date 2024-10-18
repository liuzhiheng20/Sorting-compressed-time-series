package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_64BIT;

public class DeltaGorillaDecoder extends Decoder {
    protected boolean firstValueWasRead = false;
    private byte buffer = 0;   // 写入缓冲区
    protected int bitsLeft = Byte.SIZE;   // 写入数量指标
    protected long lastValue = 0;

    protected long lastDelta   = 0;  // 存储下一个delta，从而判断有没有后面的值

    public DeltaGorillaDecoder() {
        super(TSEncoding.TS_DELTA);
    }

    @Override
    public void reset() {
        firstValueWasRead = false;
        lastValue = 0;
        lastDelta = 0;
        buffer = 0;
        bitsLeft = 0;
    }

    public void setBuffer(byte buf, int bitsleft){
        this.buffer = buf;
        this.bitsLeft = bitsleft;
    }

    public void setLastValue(long v) {
        this.lastValue = v;
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

    protected long readDelta(int bits, ByteBuffer in) {
        long delta = readLong(bits, in);
        if(bits == 9 && delta < 128 && delta > 0) delta = -delta;
        if(bits == 12 && delta < 512 && delta > 0) delta = -delta;
        if(bits == 32 && delta < 4096 && delta > 0) delta = -delta;
        return delta;
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
        if(!readBit(in)){
            lastDelta = 0;
            return true;
        }
        if(!readBit(in)) {
            lastDelta = readDelta(7, in);
            return true;
        }
        if(!readBit(in)) {
            lastDelta = readDelta(9, in);
            return true;
        }
        if(!readBit(in)) {
            lastDelta = readDelta(12, in);
            return true;
        } else {
            lastDelta = readDelta(32, in);
            if(lastDelta>Integer.MAX_VALUE){
                lastDelta = lastDelta-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
            }
            if(lastDelta == GORILLA_ENCODING_ENDING_INTEGER) {  //todo:这里用不用区分long类型和interger类型？
                return false;
            }
            return true;
        }
    }

    public int nextLen(ByteBuffer in) {
        if(!readBit(in)){
            return 1;
        }
        if(!readBit(in)) {
            return 9;
        }
        if(!readBit(in)) {
            return 12;
        }
        if(!readBit(in)) {
            return 16;
        } else {
            return 36;
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
