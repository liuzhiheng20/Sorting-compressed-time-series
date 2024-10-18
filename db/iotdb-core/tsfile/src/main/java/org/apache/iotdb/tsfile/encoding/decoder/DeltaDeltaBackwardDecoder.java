package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.DeltaDeltaBackwardEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.GORILLA_ENCODING_ENDING_INTEGER;
import static org.apache.iotdb.tsfile.common.conf.TSFileConfig.VALUE_BITS_LENGTH_64BIT;

public class DeltaDeltaBackwardDecoder extends Decoder {
    protected boolean firstValueWasRead = false;
    private byte buffer = 0;   // 写入缓冲区
    protected int bitsLeft = Byte.SIZE;   // 写入数量指标
    protected long lastDelta = 0;
    protected long lastValue = 0;

    protected long newDeltaDelta = 0;  // 存储下一个二阶delta，从而判断有没有后面的值
    protected int readPoint = 0;

    public DeltaDeltaBackwardDecoder() {
        super(TSEncoding.BACKWARD_DELTA_OF_DELTA);
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
    protected boolean readBit(ByteBuffer out){
        // 在out字节流中读取readPoint这个位置的比特值
        readPoint--;
        byte temp = out.get(readPoint/8);
        int offset = readPoint%8;
        temp = (byte) (temp<<offset);
        // readPoint--;
        if(temp < 0)
            return true;
        else
            return false;
    }

    protected long readBits(int len, ByteBuffer buf){
        // 在out字节流中读取readPoint这个位置的长度为len的值
        readPoint -= len;
        int readIndex = readPoint / 8;
        int readOffset = readPoint % 8;
        long value = 0;
        while (len > 0){
            buffer = buf.get(readIndex);
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
        //readPoint--;
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
        if(readPoint<=0) {
            return false;
        }
        long deltaDelta = 0;
        if(!readBit(in)){
            deltaDelta = 0;
            lastDelta = lastDelta-deltaDelta;
            return true;
        }
        if(!readBit(in)) {
            deltaDelta = readBits(7, in);
            if(deltaDelta>64){
                deltaDelta = deltaDelta-128;
            }
            lastDelta = lastDelta-deltaDelta;
            return true;
        }
        if(!readBit(in)) {
            deltaDelta = readBits(9, in);
            if(deltaDelta>256){
                deltaDelta = deltaDelta-512;
            }
            lastDelta = lastDelta-deltaDelta;
            return true;
        }
        if(!readBit(in)) {
            deltaDelta = readBits(12, in);
            if(deltaDelta>2048){
                deltaDelta = deltaDelta-4096;
            }
            lastDelta = lastDelta-deltaDelta;
            return true;
        } else {
            deltaDelta = readBits(32, in);
            if(deltaDelta>Integer.MAX_VALUE){
                deltaDelta = deltaDelta-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
            }
            if(deltaDelta==GORILLA_ENCODING_ENDING_INTEGER) {  //todo:这里用不用区分long类型和interger类型？
                return false;
            }
            lastDelta = lastDelta - deltaDelta;
            return true;
        }
    }

    @Override
    public final long readLong(ByteBuffer in) {  // 最终对外的接口
        if(!firstValueWasRead){
            readPoint = findLastBit(in)-1;  // 减掉最后一个的标志位
            lastValue = readBits(64, in);
            firstValueWasRead = true;
        } else{
            lastValue = lastValue-lastDelta;
        }
        return lastValue;
    }

    private int findLastBit(ByteBuffer buf){
        int index = buf.capacity();
        while(index>0){
            index--;
            if(buf.get(index)!=0){
                break;
            }
        }
        byte temp = buf.get(index);
        int offset = 8;
        while(temp%2 == 0){
            offset--;
            temp >>= 1;
        }
        // return 97;
        return 8*index+offset;
    }

    public int getBitsLeft() {
        return bitsLeft;
    }

}
