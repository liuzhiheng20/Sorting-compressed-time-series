package org.apache.iotdb.db.utils.compressedsort;

import org.apache.iotdb.tsfile.utils.TS_DELTA_data;

public class V_VARINT_decoder {
    long nowValue = 0;
    int nowNum = 0;
    int nowPos = 0;
    boolean isFirst = true;
    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};

    public V_VARINT_decoder(long nowValue, int nowNum, int nowPos){
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        if(nowNum!=0){
            isFirst = false;
        } else {
            isFirst = true;
        }
    }

    public V_VARINT_decoder(boolean isFirst, long nowValue, int nowNum, int nowPos){
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        this.isFirst = isFirst;
    }

    public void reset(V_VARINT_decoder decoderTemp) {
        this.nowValue = decoderTemp.nowValue;
        this.nowNum = decoderTemp.nowNum;
        this.nowPos = decoderTemp.nowPos;
        this.isFirst = decoderTemp.isFirst;
    }

    public void reset(long nowValue, int nowNum, int nowPos) {
        this.nowValue = nowValue;
        this.nowNum = nowNum;
        this.nowPos = nowPos;
        if(nowNum!=0){
            isFirst = false;
        } else {
            isFirst = true;
        }
    }

    public long forwardDecode(byte[] deltas, byte[] lens) {
        // 正向解码，从前往后解码
        if(isFirst){
            nowValue = readForwardValue(8, deltas);
            isFirst = false;
        } else {
            int valueLen = readValueLen(lens);
            nowValue = readForwardValue(valueLen, deltas);
        }
        nowNum++;
        return nowValue;
    }

    public long forwardDecode(TS_DELTA_data data) {
        // 正向解码，从前往后解码
        if(isFirst){
            nowValue = readForwardValue(8, data.vals);
            isFirst = false;
        } else {
            int valueLen = readValueLen(data.lens);
            nowValue = readForwardValue(valueLen, data.vals);
        }
        nowNum++;
        return nowValue;
    }

    public long backwardDecode(TS_DELTA_data data) {
        // 逆向解码，从后往前解码
        nowNum--;
        int valueLen = readValueLen(data.lens);
        nowValue = readBackwardValueDelta(valueLen, data.vals);
        return nowValue;
    }

    public long backwardDecode(byte[] deltas, byte[] lens) {
        // 逆向解码，从后往前解码
        nowNum--;
        int valueLen = readValueLen(lens);
        nowValue = readBackwardValueDelta(valueLen, deltas);
        return nowValue;
    }

    public long backwardDecode(long value, int num, int pos, byte[] deltas, byte[] lens) {
        // 逆向解码，从后往前解码
        this.nowValue = value;
        this.nowNum = num;
        this.nowPos = pos;
        nowNum--;
        int valueLen = readValueLen(lens);
        nowValue -= readForwardValue(valueLen, deltas);
        return nowValue;
    }

    public void changePos(int posDelta) {
        this.nowPos += posDelta;
    }

    public int readValueLen(byte[] lens){
//        if(nowNum==0) return 8;
//        byte temp = lens[nowNum/4];
//        temp = (byte) (temp>>(2*(3-nowNum%4)));
//        temp = (byte) (temp & 0x03);
//        if(temp == 0) return 0;
//        if(temp == 1) return 1;
//        if(temp == 2) return 2;
//        return 4;
        byte temp = lens[nowNum/4];
        temp = (byte) (temp>>(2*(3-nowNum%4)));
        temp = (byte) (temp & 0x03);
        if(temp == 0) return 8;
        if(temp == 1) return 1;
        if(temp == 2) return 2;
        return 4;
    }

    public long readForwardValue(int byteNum, byte[] deltas) {
        // 指针位置随着读的变化而变化
        long val = 0;
        long temp;
        for(int i=0; i<byteNum; i++){
            temp = deltas[nowPos];
            if(temp<0) temp = temp+256;
            val += temp*pow[i];
            nowPos++;
        }
        if(val %2 == 0)
            return val/2;
        return -(val-1)/2;
    }

    public long readBackwardValueDelta(int byteNum, byte[] deltas) {
        //先变指针，读完之后还得变指针
        nowPos = nowPos-byteNum;
        long val = readForwardValue(byteNum, deltas);
        nowPos = nowPos-byteNum;
        return val;
    }



}
