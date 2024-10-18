package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.nio.ByteBuffer;

public class VarDeltaLongDecoder extends Decoder {
    protected boolean firstValueWasRead = false;
    private int totalNum = 0;

    private long nowValue;
    private int nowNum = 0;
    protected int nowPos = 0;
    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};


    public VarDeltaLongDecoder() {
        super(TSEncoding.VAR_DELTA_LONG);
    }

    @Override
    public void reset() {
        this.firstValueWasRead = false;
        this.nowPos = 0;
        this.totalNum = 0;
        this.nowNum = 0;
    }

    /**
     * Reads the next bit and returns a boolean representing it.
     *
     * @return true if the next bit is 1, otherwise 0.
     */

    @Override
    public final boolean hasNext(ByteBuffer in) {
        if(nowNum==0){
            return true;
        }
        return (nowNum+1) < totalNum;
    }

    @Override
    public final long readLong(ByteBuffer in) {  // 最终对外的接口
        if(!firstValueWasRead){
            totalNum = ReadWriteForEncodingUtils.readUnsignedInt(in);
            nowPos = 4 + (totalNum-1)/4+1;
            firstValueWasRead = true;
            nowValue = readForwardValue(8, in.array());
        }
        else {
            int valueLen = readValueLen(in.array());
            nowValue += readForwardValue(valueLen, in.array());
        }
        nowNum++;
        return nowValue;
    }

    public int readValueLen(byte[] data){
        byte temp = data[4+nowNum/4];
        temp = (byte) (temp>>(2*(3-nowNum%4)));
        temp = (byte) (temp & 0x03);
        if(temp == 0) return 0;
        if(temp == 1) return 1;
        if(temp == 2) return 2;
        return 4;
    }

    public long readForwardValue(int byteNum, byte[] data) {
        // 指针位置随着读的变化而变化
        long delta = 0;
        for(int i=0; i<byteNum; i++){
            long temp = data[nowPos];
            if(temp<0) temp = temp+256;
            delta += temp*pow[i];
            nowPos++;
        }
        if(delta>Integer.MAX_VALUE && byteNum<=4){
            delta = delta-Integer.MAX_VALUE-Integer.MAX_VALUE-2;
        }
        return delta;
    }
}
