package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_64BIT;

public class DeltaGorillaEncoder extends Encoder {
    protected boolean firstValueWasWritten = false;
    private byte buffer = 0;   // 写入缓冲区
    protected int bitsLeft = Byte.SIZE;   // 写入数量指标
    //protected int maxPositiveDeltaLen = 0;  // 在之前成功写入的数据中，最大的正值delta所使用的编码长度
    protected long lastValue = 0;

    public DeltaGorillaEncoder() {
        super(TSEncoding.TS_DELTA);
    }

    protected void flipByte(ByteArrayOutputStream out) {
        if (bitsLeft == 0) {
            out.write(buffer);
            buffer = 0;
            bitsLeft = Byte.SIZE;
        }
    }

    public void reset() {
        firstValueWasWritten = false;
        buffer = 0;
        bitsLeft = Byte.SIZE;
        //maxPositiveDeltaLen = 0;
        lastValue = 0;
    }

    /** Stores a 0 and increases the count of bits by 1. */
    protected void skipBit(ByteArrayOutputStream out) {
        bitsLeft--;
        flipByte(out);
    }

    /** Stores a 1 and increases the count of bits by 1. */
    protected void writeBit(ByteArrayOutputStream out) {
        buffer |= (1 << (bitsLeft - 1));
        bitsLeft--;
        flipByte(out);
    }

    /**
     * Writes the given long value using the defined amount of least significant bits.
     *
     * @param value The long value to be written
     * @param bits How many bits are stored to the stream
     */
    protected void writeBits(long value, int bits, ByteArrayOutputStream out) {
        while (bits > 0) {
            int shift = bits - bitsLeft;
            if (shift >= 0) {
                buffer |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                shift = bitsLeft - bits;
                buffer |= (byte) (value << shift);
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte(out);
        }
    }

    protected void writeBits(int value, int bits, ByteArrayOutputStream out) {
        while (bits > 0) {
            int shift = bits - bitsLeft;
            if (shift >= 0) {
                buffer |= (byte) ((value >> shift) & ((1 << bitsLeft) - 1));
                bits -= bitsLeft;
                bitsLeft = 0;
            } else {
                shift = bitsLeft - bits;
                buffer |= (byte) (value << shift);
                bitsLeft -= bits;
                bits = 0;
            }
            flipByte(out);
        }
    }

    @Override
    public final void encode(long value, ByteArrayOutputStream out) {
        if (firstValueWasWritten) {
            compressValue(value, out);
        } else {
            writeFirst(value, out);
            firstValueWasWritten = true;
        }
    }

    public final void encode(long value) {
        if (firstValueWasWritten) {
            compressValue(value);
        } else {
            writeFirst(value);
            firstValueWasWritten = true;
        }
    }

    private void writeFirst(long value, ByteArrayOutputStream out) {
        lastValue = value;
        writeBits(value, VALUE_BITS_LENGTH_64BIT, out);
    }

    private void writeFirst(long value) {
        lastValue = value;
    }

    public int GorrilaTimeEncoder(long delta, boolean isNext) {
        if(delta < 0) {  // 对于负数值delta，直接用最小值编码
            return 36;
        }
        // 返回当前值在该种编码算法下编码所需要的长度
        if(delta==0){
            if(!isNext) return 1;
            else return 9;
        }
        if(delta < 128){
            if(!isNext) return 2+7;
            else return 12;
        }
        if(delta < 512){
            if(!isNext) return 3+9;
            else return 16;
        }
        if(delta < 4096){
            if(!isNext) return 4+12;
            else return 36;
        }
        return 4+32;
    }

    public void compressValue(long value, int bits, ByteArrayOutputStream out) {
        // 对于一个正数，使用指定位数编码
        if(bits == 1){
            skipBit(out);
            return;
        }
        writeBit(out);
        if (bits == 9){
            skipBit(out);
            writeBits(value,7,out);
            return;
        }
        writeBit(out);
        if (bits == 12){
            skipBit(out);
            writeBits(value,9,out);
            return;
        }
        writeBit(out);
        if(bits == 16){
            skipBit(out);
            writeBits(value,12,out);
            return;
        }
        if(bits == 36) {
            writeBit(out);
            writeBits(value,32,out);
            return;
        }

    }

    private void compressValue(long value, ByteArrayOutputStream out) {
        long newDelta = value-lastValue;
        lastValue = value;
        if(newDelta < 0){  // 对于负数值的编码发生了变化
            compressValue(newDelta, 36, out);
        } else {
            compressValue(newDelta, GorrilaTimeEncoder(newDelta, false), out);
        }
    }

    private void compressValue(long value) {
        lastValue = value;
    }

    @Override
    public void flush(ByteArrayOutputStream out) {
        // ending stream
        // encode(GORILLA_ENCODING_ENDING_INTEGER, out);
        writeBit(out);
        writeBit(out);
        writeBit(out);
        writeBit(out);
        writeBits(GORILLA_ENCODING_ENDING_INTEGER, 32, out);
        // flip the byte no matter it is empty or not
        // the empty ending byte is necessary when decoding
        bitsLeft = 0;
        flipByte(out);

        // the encoder may be reused, so let us reset it
        reset();
    }
    public int getBitsleft() {
        return bitsLeft;
    }

    public void flushBuffer(ByteArrayOutputStream out) {
        out.write(buffer);
    }
}
