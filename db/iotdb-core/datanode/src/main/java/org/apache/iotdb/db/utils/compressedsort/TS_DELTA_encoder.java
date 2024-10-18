package org.apache.iotdb.db.utils.compressedsort;

import org.apache.iotdb.tsfile.utils.TS_DELTA_data;

public class TS_DELTA_encoder {
    // 长度标志位分离
    // 以byte为单位
    long lastValue = 0;
    int valsLen = 0;  // 记录vals数组中有效数据的长度(以byte为单位)
    int valNum = 0;  // 记录目前压缩数据中已经有多少个数据了
    boolean isFirst = true;

    public void reset() {
        this.lastValue = 0;
        this.valNum = 0;
        this.valsLen = 0;
        this.isFirst = true;
    }
    public void encode(long value, TS_DELTA_data compressedData){
        if (isFirst == true) {
           writeBits(value, compressedData);
           lastValue = value;
           isFirst = false;
        } else {
            long valuetemp = value-lastValue;
            lastValue = value;
            if(valuetemp<0) {
                if(valuetemp==-1){
                    valuetemp = -1;
                }
                writeBits(valuetemp, 4, compressedData);
            } else{
                if (valuetemp == 0) {
                    writeBits(valuetemp, 0, compressedData);
                } else if (valuetemp < 256) {
                    writeBits(valuetemp, 1, compressedData);
                } else if (valuetemp < 65536) {
                    writeBits(valuetemp, 2, compressedData);
                } else {
                    writeBits(valuetemp, 4, compressedData);
                }
            }
        }
        valNum++;
    }

    public int changeEncode(int index, int begPos, long newDelta, TS_DELTA_data compressedData, int endPos) {
        // 修改在指定位置上的元素的值,数据的整体移动，最多不超过endPos这个位置（不包括endPos）
        // 返回新的长度相对于原先长度的变化
        int originalLen = getLen(index,compressedData);
        int newLen = mapDeltaToLen(newDelta);
        writeLen(index, newLen, compressedData);
        if(newLen > originalLen) {
            compressedData.rightMoveVals(newLen-originalLen, begPos+originalLen, endPos-(newLen-originalLen));
        }
        if(newLen < originalLen) {
            compressedData.leftMoveVals(originalLen-newLen, begPos+originalLen, endPos);
        }
        writeBits(newDelta, newLen, begPos, compressedData);
        return newLen - originalLen;
    }

    public int changeEncodeWithFixedLenByInd(int index, int begPos, long newDelta, int lenInd, TS_DELTA_data compressedData, int endPos) {
        // 修改在指定位置上的元素的值,数据的整体移动，最多不超过endPos这个位置（不包括endPos）
        // 新的长度的确定是通过某一个原本位置的元素的长度确定的
        int newLen = getLen(lenInd, compressedData);
        int originalLen = getLen(index,compressedData);
        writeLen(index, newLen, compressedData);
        if(newLen > originalLen) {
            compressedData.rightMoveVals(newLen-originalLen, begPos+originalLen, endPos-(newLen-originalLen));
        }
        if(newLen < originalLen) {
            compressedData.leftMoveVals(originalLen-newLen, begPos+originalLen, endPos);
        }
        writeBits(newDelta, newLen, begPos, compressedData);
        return newLen - originalLen;
    }

    public int changeEncodeWithFixedLen(int index, int begPos, long newDelta, int newLen, TS_DELTA_data compressedData, int endPos) {
        // 修改在指定位置上的元素的值,数据的整体移动，最多不超过endPos这个位置（不包括endPos）
        // 新的长度的确定是通过某一个原本位置的元素的长度确定的
        int originalLen = getLen(index,compressedData);
        writeLen(index, newLen, compressedData);
        if(newLen > originalLen) {
            compressedData.rightMoveVals(newLen-originalLen, begPos+originalLen, endPos-(newLen-originalLen));
        }
        if(newLen < originalLen) {
            compressedData.leftMoveVals(originalLen-newLen, begPos+originalLen, endPos);
        }
        writeBits(newDelta, newLen, begPos, compressedData);
        return newLen - originalLen;
    }

    public int mapDeltaToLen(long delta) {
        int newLen = 0;
        if (delta < 0) {
            newLen = 4;
        } else if (delta == 0) {
            newLen = 0;
        } else if (delta < 256) {
            newLen = 1;
        } else if (delta < 65536) {
            newLen = 2;
        } else {
            newLen = 4;
        }
        return newLen;
    }

    public void writeLen(int index, int len, TS_DELTA_data compressedData) {
        // 更改压缩数据中，指定位置数据的长度
        byte temp = 0;
        if(len == 1) temp= 1;
        if(len == 2) temp = 2;
        if(len == 4) temp = 3;
        byte unmask = (byte) (0x3 << (2*(3-index%4)));
        compressedData.lens[index/4] = (byte) (compressedData.lens[index/4]&(~unmask));
        compressedData.lens[index/4] = (byte) (compressedData.lens[index/4]|(temp<<(2*(3-index%4))));
    }

    public int getLen(int index, TS_DELTA_data compressedData) {
        // 获得压缩数据中，指定位置数据的长度
        byte unmask = (byte) (0x3 << (2*(3-index%4)));
        byte val = (byte) ((compressedData.lens[index/4]&unmask)>>(2*(3-index%4)));
        val = (byte) (val & 0x3);
        if(val==0) return 8;
        if(val == 3 || val == -1) return 4;
        return val;
    }

    private void writeBits(long value, int byteNum, TS_DELTA_data compressedData){
        // 先写上lens数组中的内容
        compressedData.checkExpand(valsLen, valNum, byteNum);
        byte temp = 0;
        if(byteNum == 1) temp= 1;
        if(byteNum == 2) temp = 2;
        if(byteNum == 4) temp = 3;
        compressedData.lens[valNum/4] = (byte) (compressedData.lens[valNum/4]|(temp<<(2*(3-valNum%4))));
        // 再写上vals数组中的内容
        while(byteNum>0) {
            compressedData.vals[valsLen] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            valsLen++;
        }
    }

    private void writeBits(long value, int byteNum, int beg, TS_DELTA_data compressedData){
        while(byteNum>0) {
            compressedData.vals[beg] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            beg++;
        }
    }

    private void writeBits(long value, TS_DELTA_data compressedData) {
        int byteNum = 8;
        while(byteNum>0) {
            compressedData.vals[valsLen] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            valsLen++;
        }
    }

    public int getValsLen() {
        return this.valsLen;
    }

    public int getValsNum() {
        return this.valNum;
    }
}
