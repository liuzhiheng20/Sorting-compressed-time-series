package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.encoding.decoder.DeltaDeltaBackwardDecoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;

public class DeltaDeltaBackwardEncoder extends Encoder{
    // 实现二次delta的逆向编码，在插入的同时进行排序
    long lastValue = -1;
    long secondLastValue = -1;
    private byte buffer = 0;   // 写入缓冲区
    protected int bitsLeft = Byte.SIZE;   // 写入数量指标
    protected int bitLen = 0;
    protected int readPoint;

    public DeltaDeltaBackwardEncoder() {
        super(TSEncoding.BACKWARD_DELTA_OF_DELTA);
    }
    protected void flipByte(ByteArrayOutputStream out) {
        if (bitsLeft == 0) {
            out.write(buffer);
            buffer = 0;
            bitsLeft = Byte.SIZE;
        }
    }

    protected void unflipByte(PublicBAOS buf) {
        // 根据需要，将out中byte数组的最后一个byte恢复成buffer
        bitsLeft = 8-bitLen%8;
        buf.setCount(bitLen/8);
        buffer = buf.getBuf()[bitLen/8];
        buffer = (byte) (buffer & (-256>>(8-bitsLeft)));
    }

    protected void reset() {
        buffer = 0;
        bitsLeft = Byte.SIZE;
        lastValue = 0;
    }
    @Override
    public void flush(ByteArrayOutputStream out) {
        // ending stream
        // encode(GORILLA_ENCODING_ENDING_INTEGER, out);
        compressDeltaDelta(-(lastValue-secondLastValue), out);
        writeBits(lastValue, 64, out);
        writeBit(out);
        bitLen += 65;
        bitsLeft = 0;
        flipByte(out);
        // the encoder may be reused, so let us reset it
        reset();
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

    protected boolean readBit(ByteArrayOutputStream out){
        // 在out字节流中读取readPoint这个位置的比特值
        readPoint--;
        PublicBAOS s = (PublicBAOS)out;
        byte temp = s.getBuf()[readPoint/8];
        int offset = readPoint%8;
        temp = (byte) (temp<<offset);
        // readPoint--;
        if(temp < 0)
            return true;
        else
            return false;
    }

    protected long readBits(int len, ByteArrayOutputStream out){
        // 在out字节流中读取readPoint这个位置的长度为len的值
        readPoint -= len;
        PublicBAOS buf = (PublicBAOS)out;
        int readIndex = readPoint / 8;
        int readOffset = readPoint % 8;
        long value = 0;
        while (len > 0){
            buffer = buf.getBuf()[readIndex];
            if (len > 8-readOffset){
                // 不能在当前一个byte读完
                byte d = (byte) (buffer & ((1 << (8-readOffset)) - 1));
                value = (value << (8-readOffset)) + (d & 0xFF);
                len -= 8-readOffset;
                readOffset = 0;
                readIndex += 1;
            } else{
                // 能够在当前一个byte读完
                byte d = (byte) ((buffer >>> (8-readOffset - len)) & ((1 << len) - 1));
                value = (value << len) + (d & 0xFF);
                len = 0;
            }
        }
        // readPoint--;
        return value;
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

    protected long readValue(PublicBAOS out) {
        // 从out数据的readPoint位置读取出一个数据（该数据是delta of delta）
        long deltaDelta = 0;
        if(!readBit(out)){
            return 0;
        }
        if(!readBit(out)) {
            deltaDelta = readBits(7, out);
            if(deltaDelta>64){
                deltaDelta = deltaDelta-128;
            }
            return deltaDelta;
        }
        if(!readBit(out)) {
            deltaDelta = readBits(9, out);
            if(deltaDelta>256){
                deltaDelta = deltaDelta-512;
            }
            return deltaDelta;
        }
        if(!readBit(out)) {
            deltaDelta = readBits(12, out);;
            if(deltaDelta>2048){
                deltaDelta = deltaDelta-4096;
            }
            return deltaDelta;
        } else {
            deltaDelta = readBits(32, out);;
            if(deltaDelta>Integer.MAX_VALUE){
                deltaDelta = deltaDelta-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
            }
            return deltaDelta;
        }
    }

    @Override
    public final void encode(long value, ByteArrayOutputStream out) {
        if (secondLastValue>0) {
            compressValue(value, out);
        } else {
            if(value>lastValue){
                secondLastValue = lastValue;
                lastValue = value;
            } else {
                secondLastValue = value;
            }
        }
    }

    private void compressValue(long value, ByteArrayOutputStream out) {
        if (value >= lastValue){
            compressOrderValue(value, out);
        } else{
            compressUnorderValue(value, out);
        }
    }

    private void compressDeltaDelta(long deltaDelta, ByteArrayOutputStream out){
        if(deltaDelta == 0){
            bitLen += 1;
            skipBit(out);
            return;
        }
        if (deltaDelta >= -63 && deltaDelta <= 64){
            bitLen += 9;
            writeBits(deltaDelta,7,out);
            skipBit(out);
            writeBit(out);
            return;
        }
        if (deltaDelta >= -255 && deltaDelta <= 256){
            bitLen += 12;
            writeBits(deltaDelta,9,out);
            skipBit(out);
            writeBit(out);
            writeBit(out);
            return;
        }
        if (deltaDelta >= -2047 && deltaDelta <= 2048){
            bitLen += 16;
            writeBits(deltaDelta,12,out);
            skipBit(out);
            writeBit(out);
            writeBit(out);
            writeBit(out);
        } else{
            bitLen += 36;
            writeBits(deltaDelta,32,out);
            writeBit(out);
            writeBit(out);
            writeBit(out);
            writeBit(out);
        }
    }

    private void compressOrderValue(long value, ByteArrayOutputStream out) {
        long newDelta = value-lastValue;
        long deltaDelta = newDelta-(lastValue-secondLastValue);
        secondLastValue = lastValue;
        lastValue = value;
        // 更改写入顺序，标志位最后写入，方便从后向前的解码
        compressDeltaDelta(deltaDelta, out);
    }

    private void compressUnorderValue(long value, ByteArrayOutputStream out) {

        readPoint = bitLen;
        if(value > secondLastValue){
            if(secondLastValue == 0){
                // 此时只有一个数据
                secondLastValue = value;
            } else if (readPoint==0) {
                // 此时只有两个数据
                long valueTemp = lastValue;
                lastValue = value;
                compressOrderValue(valueTemp, out);
            } else {
                bitsLeft = 0;
                flipByte(out);  // 先把所有数据写入out中
                PublicBAOS buf = (PublicBAOS)out;
                // 此时有三个即以上的数据
                long dd = readValue(buf);
                long nowValue = secondLastValue - (lastValue-secondLastValue-dd);
                // 恢复压缩数据的状态
                bitLen = readPoint;
                buf.setCount(bitLen/8);
                bitsLeft = 8-bitLen%8;
                unflipByte(buf);
                // 在源数据上压缩
                long secondLastValueTemp = secondLastValue;
                long lastValueTemp = lastValue;
                secondLastValue = nowValue;
                lastValue = secondLastValueTemp;
                compressOrderValue(value, out);
                secondLastValue = secondLastValueTemp;
                lastValue = value;
                compressOrderValue(lastValueTemp, out);
            }
        } else{
            bitsLeft = 0;
            flipByte(out);  // 先把所有数据写入out中
            PublicBAOS buf = (PublicBAOS)out;
            long lastValueTemp = lastValue;
            long lastSecondValueTemp = secondLastValue;
            long nowValue = 0;
            long dd = 0;  // delta of delta
            int readPointTemp=0;
            while(readPoint>0){
                 readPointTemp = readPoint;
                 dd = readValue(buf);
                 nowValue = lastSecondValueTemp - (lastValueTemp-lastSecondValueTemp-dd);
                 if(nowValue<value){
                     // 确定了插入位置
                     break;
                 } else{
                     readPointTemp = readPoint;
                     lastValueTemp = lastSecondValueTemp;
                     lastSecondValueTemp = nowValue;
                 }
            }
            long valueBefore = 0;
            if(readPoint>0){
                // lastValueTemp = lastSecondValueTemp;
                // lastSecondValueTemp = nowValue;
                // valueBefore = lastSecondValueTemp - (lastValueTemp-lastSecondValueTemp-readValue(buf));
                valueBefore = nowValue - (lastSecondValueTemp-nowValue-readValue(buf));
            }
            DeltaDeltaBackwardEncoder encodertemp = new DeltaDeltaBackwardEncoder();
            PublicBAOS outtemp = new PublicBAOS();
            if(valueBefore != 0 && valueBefore < value){
                encodertemp.encode(valueBefore ,outtemp);
            }
            if(nowValue != 0 && nowValue < value){
                encodertemp.encode(nowValue ,outtemp);
            }
            encodertemp.encode(value ,outtemp);
            encodertemp.encode(lastSecondValueTemp ,outtemp);
            encodertemp.encode(lastValueTemp ,outtemp);
            encodertemp.bitsLeft = 0;
            encodertemp.flipByte(outtemp);

            int moveLen = (readPointTemp-readPoint)-encodertemp.bitLen;  // 原本数据的长度减去新产生的数据的长度
            if (moveLen < 0){
                for(int o=0; o<-moveLen; o++)
                {
                    buf.write(0);
                }
                buf.rightMoveBuffer(readPointTemp, -moveLen);
            }
            if(moveLen > 0) {
                buf.leftMoveBuffer(readPointTemp, moveLen);
            }
            buf.writeBuffer(readPoint, encodertemp.bitLen, outtemp.getBuf());
            bitLen -= moveLen;
            unflipByte(buf);
        }
    }

}
