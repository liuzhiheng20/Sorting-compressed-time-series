package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.io.ByteArrayOutputStream;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.*;

public class DeltaDeltaLongEncoder extends Encoder {
    protected boolean firstValueWasWritten = false;
    private byte buffer = 0;   // 写入缓冲区
    protected int bitsLeft = Byte.SIZE;   // 写入数量指标
    protected long lastDelta = 0;
    protected long lastValue = 0;

    public DeltaDeltaLongEncoder() {
        super(TSEncoding.DELTA_OF_DELTA);
    }

    protected void flipByte(ByteArrayOutputStream out) {
        if (bitsLeft == 0) {
            out.write(buffer);
            buffer = 0;
            bitsLeft = Byte.SIZE;
        }
    }

    protected void reset() {
        firstValueWasWritten = false;
        buffer = 0;
        bitsLeft = Byte.SIZE;
        lastDelta = 0;
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

    private void compressValue(long value, ByteArrayOutputStream out) {
        long newDelta = value-lastValue;
        long deltaDelta = newDelta-lastDelta;
        lastValue = value;
        lastDelta = newDelta;
        if(deltaDelta == 0){
            skipBit(out);
            return;
        }
        if (deltaDelta >= -63 && deltaDelta <= 64){
            writeBit(out);
            skipBit(out);
            writeBits(deltaDelta,7,out);
            return;
        }
        if (deltaDelta >= -255 && deltaDelta <= 256){
            writeBit(out);
            writeBit(out);
            skipBit(out);
            writeBits(deltaDelta,9,out);
            return;
        }
        writeBit(out);
        writeBit(out);
        writeBit(out);
        if (deltaDelta >= -2047 && deltaDelta <= 2048){
            skipBit(out);
            writeBits(deltaDelta,12,out);
        } else{
            writeBit(out);
            writeBits(deltaDelta,32,out);
        }
    }

    private void compressValue(long value) {
        long newDelta = value - lastValue;
        long deltaDelta = newDelta - lastDelta;
        lastValue = value;
        lastDelta = newDelta;
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
