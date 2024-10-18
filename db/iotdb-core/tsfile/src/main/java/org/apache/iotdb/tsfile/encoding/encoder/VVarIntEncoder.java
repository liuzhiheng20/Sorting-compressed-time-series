package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class VVarIntEncoder extends Encoder {
    int valsLen = 0;  // 记录vals数组中有效数据的长度(以byte为单位)
    int valNum = 0;  // 记录目前压缩数据中已经有多少个数据了
    boolean isFirst = true;

    public VVarIntEncoder() {
        super(TSEncoding.VARLONG);
    }

    public void encode(long value, TS_DELTA_data compressedData){
        if (isFirst) {
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, compressedData);
            isFirst = false;
        }
        else {
            int byteNum = mapDataToLen(value);
            if (value <= 0) value = -2 * value + 1;
            else value = 2 * value;
            writeBits(value, byteNum, compressedData);
        }
        valNum++;
    }

    public int changeEncode(int index, int begPos, long newData, TS_DELTA_data compressedData, int endPos) {
        // 修改在指定位置上的元素的值,数据的整体移动，最多不超过endPos这个位置（不包括endPos）
        // 返回新的长度相对于原先长度的变化
        int originalLen = getLen(index,compressedData);
        int newLen = mapDataToLen(newData);
        writeLen(index, newLen, compressedData);
        if(newLen > originalLen) {
            compressedData.rightMoveVals(newLen-originalLen, begPos+originalLen, endPos-(newLen-originalLen));
        }
        if(newLen < originalLen) {
            compressedData.leftMoveVals(originalLen-newLen, begPos+originalLen, endPos);
        }
        writeBits(newData, newLen, begPos, compressedData);
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

    public int mapDataToLen(long data) {
        // 0对应到-1，负数依次减1
        if (data <= 0){
            data = data-1;
            data = -data;
        }
        if(data<128) return 1;
        if(data<32768) return 2;
        if(data-1<Integer.MAX_VALUE) return 4;
        return 8;
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
        if(val == 3 || val == -1) return 4;
        if(val == 0) return 8;
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

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        return;
    }

    public int getValsNum() {
        return this.valNum;
    }

    public int getValsLen() {
        return this.valsLen;
    }
}
