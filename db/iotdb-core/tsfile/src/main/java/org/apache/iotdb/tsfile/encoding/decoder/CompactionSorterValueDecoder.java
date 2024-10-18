package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.utils.CompressedPageData;

import java.util.LinkedList;

public class CompactionSorterValueDecoder {
    int nowNum = 0;   // 用来确认len数组
    int nowIndex = 0;
    int nowPageCount = 0;
    int nowValueIndex = 0;   // 用来确认val数组
    int nowValuePos = 0;
    LinkedList<CompressedPageData> data;
    byte[] lens;
    byte[] varInts;
    long[] pow = new long[] {1, 256, 65536, 16777216, 4294967296L, (long) Math.pow(256, 5), (long) Math.pow(256, 6), (long) Math.pow(256, 7)};

    public CompactionSorterValueDecoder(int nowIndex, LinkedList<CompressedPageData> sortedPageList){
        // 设置读的位置为sortedPageList里面第nowIndex位置的元素，并且指针指向第一个元素
        CompressedPageData nowPage = sortedPageList.get(nowIndex);
        this.nowNum = 0;
        this.nowIndex = nowIndex;
        this.varInts = nowPage.getValueData().vals;
        this.lens = nowPage.getValueData().lens;
        this.nowPageCount = nowPage.getCount();
        this.nowValuePos = 0;
        this.nowValueIndex = nowIndex;
        this.data = sortedPageList;
    }


    public long forwardDecode() {
        // 正向解码，从前往后解码
        int valueLen = readValueLen();
        long nowValue = readForwardValueDelta(valueLen);
        nowNum++;
        if(nowNum >= this.nowPageCount){
            this.nowIndex++;
            this.nowNum=0;
            if(this.nowIndex<data.size()){
                this.lens = data.get(nowIndex).getValueData().lens;
                this.nowPageCount = data.get(nowIndex).getCount();
            }
        }
        return nowValue;
    }

    public long backwardDecode() {
        // 逆向解码，从后往前解码
        nowNum--;
        if(nowNum < 0) {
            this.nowIndex--;
            this.lens = data.get(nowIndex).getValueData().lens;
            this.nowPageCount = data.get(nowIndex).getCount();
            this.nowNum = this.nowPageCount-1;
        }
        int valueLen = readValueLen();
        return readBackwardValueDelta(valueLen);
    }

    public int readValueLen(){
        byte temp = lens[nowNum/4];
        temp = (byte) (temp>>(2*(3-nowNum%4)));
        temp = (byte) (temp & 0x03);
        if(temp == 0) return 8;
        if(temp == 3) return 4;
        return temp;
    }

    public long readForwardValueDelta(int byteNum) {
        // 指针位置随着读的变化而变化
        long delta = 0;
        for(int i=0; i<byteNum; i++){
            long temp = varInts[nowValuePos];
            if(temp<0) temp = temp+256;
            delta += temp*pow[i];
            nowValuePos++;
            if(nowValuePos==varInts.length){
                nowValueIndex++;
                nowValuePos = 0;
                if(nowValueIndex < data.size()){
                    this.varInts = data.get(nowValueIndex).getValueData().vals;
                }
            }
        }
        if(delta %2 == 0)
            return delta/2;
        return -(delta-1)/2;
    }

    public long readBackwardValueDelta(int byteNum) {
        //先变指针，读完之后还得变指针
        nowValuePos = nowValuePos-byteNum;
        if (nowValuePos<0){
            nowValueIndex--;
            this.varInts = data.get(nowValueIndex).getValueData().vals;
            nowValuePos += data.get(nowValueIndex).getPageValueLen();
        }
        long delta = readForwardValueDelta(byteNum);
        nowValuePos = nowValuePos-byteNum;
        if (nowValuePos<0){
            nowValueIndex--;
            this.varInts = data.get(nowValueIndex).getValueData().vals;
            nowValuePos += data.get(nowValueIndex).getPageValueLen();
        }
        return delta;
    }


    public int getNowNum() {
        return nowNum;
    }

    public int getNowIndex() {
        return nowIndex;
    }

    public int getNowValuePos() {
        return nowValuePos;
    }

    public int getNowValueIndex() {
        return nowValueIndex;
    }

    public boolean hasForwardNext() {
        return this.nowIndex < data.size();
    }

    public boolean hasBackwardNext() {
        return this.nowIndex != 0 || this.nowNum != 0;
    }
}
