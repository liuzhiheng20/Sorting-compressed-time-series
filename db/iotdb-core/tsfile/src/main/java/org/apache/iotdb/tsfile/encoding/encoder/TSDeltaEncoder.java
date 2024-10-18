package org.apache.iotdb.tsfile.encoding.encoder;

import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.CompressedPageData;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.LinkedList;

public class TSDeltaEncoder extends Encoder {
    long lastValue = 0;
    int valsLen = 0;  // 记录vals数组中有效数据的长度(以byte为单位)
    int valNum = 0;  // 记录目前压缩数据中已经有多少个数据了
    boolean isFirst = true;

    public TSDeltaEncoder() {
        super(TSEncoding.VAR_DELTA_LONG);
    }

    public void reset() {
        this.lastValue = 0;
        this.valNum = 0;
        this.valsLen = 0;
        this.isFirst = true;
    }
    public void encode(long value, TS_DELTA_data compressedData){
        if (isFirst) {
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
                writeBits(valuetemp, 8, compressedData);
            } else{
                if (valuetemp < 256) {
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

    public int changeEncodeWithFixedLen(boolean isTime, LinkedList<CompressedPageData> sortedPageList, int lensIndex, int lensNum, int valsIndex, int valsPos, long newDelta, int newLen, int endValsIndex, int endValsPos) {
        // 修改在指定位置上的元素的值,数据的整体移动，最多不超过endPos这个位置（不包括endPos）
        // 新的长度的确定是通过某一个原本位置的元素的长的
        TS_DELTA_data lenData;
        if(isTime) lenData = sortedPageList.get(lensIndex).getTimeData();
        else lenData = sortedPageList.get(lensIndex).getValueData();
        int originalLen = getLen(lensNum, lenData);
        writeLen(lensNum, newLen, lenData);
        if(newLen > originalLen) {
            rightMoveVals(isTime, sortedPageList, valsIndex, valsPos, endValsIndex, endValsPos, newLen-originalLen);
        }
        if(newLen < originalLen) {
            leftMoveVals(isTime, sortedPageList, valsIndex, valsPos, endValsIndex, endValsPos, originalLen-newLen);
        }
        TS_DELTA_data valData;
        if(isTime) valData = sortedPageList.get(valsIndex).getTimeData();
        else valData = sortedPageList.get(valsIndex).getValueData();
        writeBits(isTime, sortedPageList, valsIndex, newDelta, newLen, valsPos, valData);
        return newLen - originalLen;
    }

    public void rightMoveVals(boolean isTime, LinkedList<CompressedPageData> sortedPageList, int begIndex, int begPos, int endIndex, int endPos, int len) {
        // 现在rightSide是真实会移动的最后位置
        // 修改为 影响到的最后位置
        byte[] data = new byte[0];
        if(endIndex<sortedPageList.size()) {
            if(isTime) data = sortedPageList.get(endIndex).getTimeData().vals;
            else data = sortedPageList.get(endIndex).getValueData().vals;
        }
        int rightSidePos = endPos-len;
        int rightSideIndex = endIndex;
        byte[] rightSideData = data;
        while(rightSidePos < 0){
            rightSideIndex--;
            if(isTime) rightSideData = sortedPageList.get(rightSideIndex).getTimeData().vals;
            else rightSideData = sortedPageList.get(rightSideIndex).getValueData().vals;
            rightSidePos += rightSideData.length;
        }

        while(true){
            rightSidePos--;
            endPos--;
            if(rightSidePos<0) {
                rightSideIndex--;
                if(isTime) rightSideData = sortedPageList.get(rightSideIndex).getTimeData().vals;
                else rightSideData = sortedPageList.get(rightSideIndex).getValueData().vals;
                rightSidePos = rightSideData.length-1;
            }
            if(endPos<0) {
                endIndex--;
                if(isTime) data = sortedPageList.get(endIndex).getTimeData().vals;
                else data = sortedPageList.get(endIndex).getValueData().vals;
                endPos = data.length-1;
            }
            data[endPos] = rightSideData[rightSidePos];
            if(rightSideIndex==begIndex && rightSidePos == begPos){
                break;
            }
        }
    }

    public void leftMoveVals(boolean isTime, LinkedList<CompressedPageData> sortedPageList, int leftSideIndex, int leftSidePos, int endIndex, int endPos, int len) {
        // 给出的参数是移动影响到的左侧边界的位置，并不是开始移动的位置
        byte[] leftSideData;
        if(isTime) leftSideData = sortedPageList.get(leftSideIndex).getTimeData().vals;
        else leftSideData = sortedPageList.get(leftSideIndex).getValueData().vals;
        int begPos = leftSidePos+len;
        int begIndex = leftSideIndex;
        while (begPos>=leftSideData.length) {
            if(isTime) begPos -= sortedPageList.get(leftSideIndex).getTimeData().vals.length;
            else begPos -= sortedPageList.get(leftSideIndex).getValueData().vals.length;
            begIndex++;
        }
        byte[] data;
        if(isTime) data = sortedPageList.get(begIndex).getTimeData().vals;
        else data = sortedPageList.get(begIndex).getValueData().vals;
        while(true){
            leftSideData[leftSidePos] = data[begPos];
            leftSidePos++;
            begPos++;
            if(begPos==data.length){
              begIndex++;
              begPos=0;
              // todo:更新data数组
              if(begIndex<sortedPageList.size()){
                  if(isTime) data = sortedPageList.get(begIndex).getTimeData().vals;
                  else data = sortedPageList.get(begIndex).getValueData().vals;
              }
            }
            if(leftSidePos == leftSideData.length){
              leftSideIndex++;
              leftSidePos = 0;
              // todo:更新leftSideData数组
              if(leftSideIndex<sortedPageList.size()) {
                  if(isTime) leftSideData = sortedPageList.get(leftSideIndex).getTimeData().vals;
                  else leftSideData = sortedPageList.get(leftSideIndex).getValueData().vals;
              }
            }
            if(leftSidePos==endPos && leftSideIndex == endIndex){
              break;
            }
        }
    }

    public int mapDeltaToLen(long delta) {  //提供1、2、4、8四种长度的选择
        int newLen = 0;
        if (delta < 0) {
            newLen = 8;
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
        byte temp = compressedData.lens[index/4];
        temp = (byte) (temp>>(2*(3-index%4)));
        temp = (byte) (temp & 0x03);
        if(temp == 0) return 8;
        if(temp == 1) return 1;
        if(temp == 2) return 2;
        return 4;
        // 获得压缩数据中，指定位置数据的长度
//        byte unmask = (byte) (0x3 << (2*(3-index%4)));
//        byte val = (byte) ((compressedData.lens[index/4]&unmask)>>(2*(3-index%4)));
//        val = (byte) (val & 0x3);
//        if(val == 3 || val == -1) return 4;
//        return val;
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

    private void writeBits(boolean isTime, LinkedList<CompressedPageData> sortedPageList, int valsIndex, long value, int byteNum, int beg, TS_DELTA_data compressedData){
        while(byteNum>0) {
            compressedData.vals[beg] = (byte) (value & 0xFFL);
            value = value>>8;
            byteNum--;
            beg++;
            if (beg == compressedData.getLen()) {
                beg = 0;
                valsIndex++;
                if(isTime) compressedData = sortedPageList.get(valsIndex).getTimeData();
                else compressedData = sortedPageList.get(valsIndex).getValueData();
            }
        }
    }

    private void writeBits(long value, int byteNum, int beg, TS_DELTA_data compressedData) {
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

    @Override
    public void flush(ByteArrayOutputStream out) throws IOException {
        return;
    }
}
