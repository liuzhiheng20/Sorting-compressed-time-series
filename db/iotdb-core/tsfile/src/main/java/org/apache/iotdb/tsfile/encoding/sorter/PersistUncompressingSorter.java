package org.apache.iotdb.tsfile.encoding.sorter;

import org.apache.iotdb.tsfile.encoding.decoder.CompactionSorterTimeDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.CompactionSorterValueDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.VarDeltaLongDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.VarLongDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSDeltaEncoder;
import org.apache.iotdb.tsfile.utils.CompressedPageData;

import java.util.Iterator;
import java.util.LinkedList;

public class PersistUncompressingSorter {
    LinkedList<CompressedPageData> sortedPageList;
    long sortedPageMaxTime=Integer.MIN_VALUE;
    int timeReadPointIndex = 0;  // 读指针在list中的位置
    int timeReadPointPos = 0;  // 读指针在page中的偏移
    int valueReadPointIndex = 0;
    int valueReadPointPos = 0;
    byte[] dataMoveBegVals;
    byte[] dataMoveEndVals;
    int dataMoveBegIndex = -1;
    int dataMoveEndIndex = -1;
    int dataMoveBegSize = 0;
    int dataMoveEndSize = 0;

    // 首先把page中的第一个元素改成负数的编码
    VarLongDecoder valueReaderBefore;  // 读newpage的reader
    VarLongDecoder valueReaderAfter;  // 读oldpage的reader
    VarDeltaLongDecoder timeReaderBefore;
    VarDeltaLongDecoder timeReaderAfter;

    public PersistUncompressingSorter(LinkedList<CompressedPageData> SortedPageList) {
        this.sortedPageList = SortedPageList;
        for(int i=0; i<SortedPageList.size()-1; i++) {
            if(SortedPageList.get(i).getMaxTime()>this.sortedPageMaxTime) {
                this.sortedPageMaxTime = SortedPageList.get(i).getMaxTime();
            }
        }
    }

    public void addPage(CompressedPageData newPage) {
        if(sortedPageList.isEmpty()){   //因为数据的空间大小不变，如果list中没有有用的数据的话，本身list就是空的
            sortedPageList.add(newPage);
        } else {
            int index = 0;
            while (index<sortedPageList.size()) {  //先将没有overlap的page删除
                CompressedPageData oldPage = sortedPageList.get(index);
                if(oldPage.getIsInSorter()){
                    if(oldPage.getMaxTime()<newPage.getMinTime()) {
                        deletePage(index, oldPage.getPageTimeLen(), oldPage.getPageValueLen());
                    } else {
                        break;
                    }
                } else {
                    index++;
                }
            }
            sortedPageList.add(newPage);
            sortPage(newPage);
        }
    }

    public void deletePage(int index, int deleteTimeLen, int deleteValueLen){
        // 从sortedPageList中从前往后删除
        // 对于时间列和数值列，删除的长度分别是deleteTimeLen和deleteValueLen
        while (deleteTimeLen>0 || deleteValueLen>0) {
            if(deleteTimeLen>0 && timeReadPointIndex == 0) {
                if (sortedPageList.get(0).getPageTimeLen()-timeReadPointPos <= deleteTimeLen) {  //从时间列考虑可以删除第一个page
                    timeReadPointIndex++;
                    timeReadPointPos = 0;
                    deleteTimeLen -= sortedPageList.get(0).getPageTimeLen()-timeReadPointPos;
                } else {  // 从时间列考虑不能删除第一个page
                    timeReadPointPos += deleteTimeLen;
                    deleteTimeLen=0;
                }
            }
            if(deleteValueLen>0 && valueReadPointIndex == 0) {
                if (sortedPageList.get(0).getPageValueLen()-valueReadPointPos <= deleteValueLen) {  //从数值列考虑可以删除第一个page
                    valueReadPointIndex++;
                    valueReadPointPos = 0;
                    deleteValueLen -= sortedPageList.get(0).getPageValueLen()-valueReadPointPos;
                } else {  // 从数值列考虑不能删除第一个page
                    valueReadPointPos += deleteValueLen;
                    deleteTimeLen=0;
                }
            }
            if(timeReadPointIndex >0 && valueReadPointIndex>0){
                sortedPageList.remove(0);
                timeReadPointIndex--;
                valueReadPointIndex--;
            }
        }
    }

    public void sortPage(CompressedPageData newPage) {  // newpage是新加入pagelist里面的元素
        if(newPage.getMinTime() > this.sortedPageMaxTime) {
            // 如果新的page和之前的数据没有overlap，则直接修改newpage开始的编码，用8个bit来存储delta
            TSDeltaEncoder timeChanger = new TSDeltaEncoder();
            timeChanger.changeEncodeWithFixedLen(0, 0, newPage.getMinTime() - this.sortedPageMaxTime, 8, newPage.getTimeData(), newPage.getPageTimeLen());
            return;
        }
        CompactionSorterTimeDecoder timeForwardReader = new CompactionSorterTimeDecoder(sortedPageList.size()-1, 0, sortedPageList);
        CompactionSorterTimeDecoder timeBackwardReader = new CompactionSorterTimeDecoder(sortedPageList.size()-1, this.sortedPageMaxTime, sortedPageList);
        CompactionSorterValueDecoder valueForwardReader = new CompactionSorterValueDecoder(sortedPageList.size()-1, sortedPageList);
        CompactionSorterValueDecoder valueBackwardReader = new CompactionSorterValueDecoder(sortedPageList.size()-1, sortedPageList);
        long valueTempTime = 0;
        timeForwardReader.forwardDecode();
        valueForwardReader.forwardDecode();
        while (timeForwardReader.getNowValue() < sortedPageMaxTime) {
            valueTempTime = timeForwardReader.getNowValue();
            while (timeBackwardReader.backwardDecode() > valueTempTime){
                 valueBackwardReader.backwardDecode();
            }
            valueBackwardReader.backwardDecode();
            if(persistentSortOpeHoldLen(newPage, timeForwardReader, timeBackwardReader, valueForwardReader, valueBackwardReader) < -50) {
                return;
            }
            while (timeBackwardReader.getNowValue() < timeForwardReader.getNowValue()) {
                timeBackwardReader.forwardDecode();
                valueBackwardReader.forwardDecode();
            }
            if(timeForwardReader.getNowValue() == timeBackwardReader.getNowValue()) return;
        }
        if(newPage.getMaxTime()>this.sortedPageMaxTime){
            this.sortedPageMaxTime = newPage.getMaxTime();
        }
    }

    public int persistentSortOpeHoldLen(CompressedPageData newPage, CompactionSorterTimeDecoder timeForwardDecoder, CompactionSorterTimeDecoder timeBackwardDecoder, CompactionSorterValueDecoder valueForwardDecoder, CompactionSorterValueDecoder valueBackwardDecoder) {
        // 长度不变的压缩数据的一次基本操作(减少数据整体移动的开销)
        int upBoundIndex = timeBackwardDecoder.getNowIndex();
        int upBoundNum = timeBackwardDecoder.getNowNum();
        int upBoundValueIndex = timeBackwardDecoder.getNowValueIndex();
        int upBoundValuePos = timeBackwardDecoder.getNowValuePos();
//        int v_upBoundIndex = valueBackwardDecoder.getNowIndex();
//        int v_upBoundNum = valueBackwardDecoder.getNowNum();
        int v_upBoundValueIndex = valueBackwardDecoder.getNowValueIndex();
        int v_upBoundValuePos = valueBackwardDecoder.getNowValuePos();
        long upBound = timeBackwardDecoder.forwardDecode();
        timeForwardDecoder.backwardDecode();
        int valueTempIndex = timeForwardDecoder.getNowIndex();
        int valueTempNum = timeForwardDecoder.getNowNum();
        int valueTempValueIndex = timeForwardDecoder.getNowValueIndex();
        int valueTempValuePos = timeForwardDecoder.getNowValuePos();
        valueForwardDecoder.backwardDecode();
        int v_valueTempIndex = valueForwardDecoder.getNowIndex();
        int v_valueTempNum = valueForwardDecoder.getNowNum();
        int v_valueTempValueIndex = valueForwardDecoder.getNowValueIndex();
        int v_valueTempValuePos = valueForwardDecoder.getNowValuePos();
        long valueTempTime = timeForwardDecoder.forwardDecode();
        valueForwardDecoder.forwardDecode();
        int hasNext = 0;
        if(!timeForwardDecoder.hasForwardNext()){
            hasNext = -100;
        }

        long tailTime = valueTempTime;
        int newHeadPos = timeForwardDecoder.getNowValuePos();
        int newHeadIndex = timeForwardDecoder.getNowValueIndex();
        int newHeadLenNum = timeForwardDecoder.getNowNum();
        int v_newHeadPos = valueForwardDecoder.getNowValuePos();
        int v_newHeadIndex = valueForwardDecoder.getNowValueIndex();
        int v_newHeadLenNum = valueForwardDecoder.getNowNum();
        while (timeForwardDecoder.hasForwardNext() && timeForwardDecoder.forwardDecode() < upBound) {
            valueForwardDecoder.forwardDecode();
            tailTime = timeForwardDecoder.getNowValue();
            newHeadPos = timeForwardDecoder.getNowValuePos();
            newHeadIndex = timeForwardDecoder.getNowValueIndex();
            newHeadLenNum = timeForwardDecoder.getNowNum();
            v_newHeadPos = valueForwardDecoder.getNowValuePos();
            v_newHeadIndex = valueForwardDecoder.getNowValueIndex();
            v_newHeadLenNum = valueForwardDecoder.getNowNum();
        }
        if(valueForwardDecoder.hasForwardNext()) valueForwardDecoder.forwardDecode();
        // 直接修改三个位置元素的delta
        TSDeltaEncoder timeChanger = new TSDeltaEncoder();
        int endTimePosition = timeForwardDecoder.getNowValuePos();
        if (endTimePosition==0) {
            endTimePosition = newPage.getPageTimeLen();
        }
        int valueTempVar = timeChanger.changeEncodeWithFixedLen(valueTempNum, valueTempValuePos, valueTempTime-timeBackwardDecoder.backwardDecode(), timeBackwardDecoder.readValueLen(), newPage.getTimeData(), endTimePosition);
        //valueBackwardDecoder.backwardDecode();
        int upBoundVar;
        if(timeForwardDecoder.getNowValue() > upBound)  {// 有newHead,upBound不在new page中
            upBoundVar = timeChanger.changeEncodeWithFixedLen(true,sortedPageList, upBoundIndex, upBoundNum, upBoundValueIndex, upBoundValuePos, upBound - tailTime, timeForwardDecoder.readNowValueLen(), timeForwardDecoder.getNowValueIndex(), timeForwardDecoder.getNowValuePos());
        }
        else {
            upBoundVar = timeChanger.changeEncodeWithFixedLen(true, sortedPageList, upBoundIndex, upBoundNum, upBoundValueIndex, upBoundValuePos, upBound - tailTime, 8, timeForwardDecoder.getNowValueIndex(), timeForwardDecoder.getNowValuePos());
        }
        int newHeadVar = 0;
        valueTempValuePos += upBoundVar;
        if(timeForwardDecoder.getNowValue() > upBound) {
            newHeadPos += upBoundVar+valueTempVar;
            if(timeForwardDecoder.getNowNum()!=0) newHeadVar = timeChanger.changeEncodeWithFixedLen(timeForwardDecoder.getNowNum()-1, newHeadPos, timeForwardDecoder.getNowValue()-sortedPageMaxTime, 8, newPage.getTimeData(), timeForwardDecoder.getNowValuePos());
            else newHeadVar = timeChanger.changeEncodeWithFixedLen(newPage.getCount()-1, newHeadPos, timeForwardDecoder.getNowValue()-sortedPageMaxTime, 8, newPage.getTimeData(), newPage.getPageTimeLen());
        } else {
            newHeadPos += upBoundVar+valueTempVar;
        }
        // 中间部分的元素整体移动
        dataMove(true, upBoundValueIndex, upBoundValuePos, valueTempValueIndex, valueTempValuePos, newHeadIndex, newHeadPos);
        dataMove(false, v_upBoundValueIndex, v_upBoundValuePos, v_valueTempValueIndex, v_valueTempValuePos, v_newHeadIndex, v_newHeadPos);
        lenDataMove(true, timeBackwardDecoder.getNowIndex(), timeBackwardDecoder.getNowNum(), valueTempIndex, valueTempNum, newHeadIndex, newHeadLenNum);
        lenDataMove(false, valueBackwardDecoder.getNowIndex(), valueBackwardDecoder.getNowNum(), v_valueTempIndex, v_valueTempNum, v_newHeadIndex, v_newHeadLenNum);
        return valueTempVar+upBoundVar+newHeadVar+hasNext;  // 应该返回经过一次排序基本操作后，block的总体的长度变化
    }

    public void dataMove(boolean isTime, int begIndex, int begPos, int midIndex, int midPos, int endIndex, int endPos) {   // 把mid到end之间的元素移动到beg之前
        dataMoveEndIndex = -1;
        dataMoveBegIndex = -1;
        if(midPos<0) {
            midIndex--;
            if(isTime) midPos += sortedPageList.get(midIndex).getPageTimeLen();
            else midPos += sortedPageList.get(midIndex).getPageValueLen();
        }
        int beg = begPos;
        int mid = midPos;
        for(int i=begIndex; i<midIndex;i++) {
            if(isTime) mid += sortedPageList.get(i).getTimeData().vals.length;
            else mid += sortedPageList.get(i).getValueData().vals.length;
        }
        int end = endPos;
        for(int i=begIndex; i<endIndex;i++) {
            if(isTime) end += sortedPageList.get(i).getTimeData().vals.length;
            else end += sortedPageList.get(i).getValueData().vals.length;
        }
        if(mid == end || mid == beg) return;
        while(mid>beg && end>mid) {
            if(end-mid <= mid-beg) {
                int endBegPos = endPos - (end-mid);
                int endBegIndex = endIndex;
                while(endBegPos<0){
                    endBegIndex--;
                    if(isTime) endBegPos += sortedPageList.get(endBegIndex).getPageTimeLen();
                    else endBegPos += sortedPageList.get(endBegIndex).getPageValueLen();
                }
                dataExchange(isTime, begIndex, begPos, endBegIndex, endBegPos, end-mid);
                beg = beg+end-mid;
                begPos += end-mid;
                int upBoundLen = 0;
                while(true) {
                    if(isTime) upBoundLen = sortedPageList.get(begIndex).getPageTimeLen();
                    else upBoundLen = sortedPageList.get(begIndex).getPageValueLen();
                    if(begPos >= upBoundLen) {
                        begPos -= upBoundLen;
                        begIndex++;
                    } else {
                        break;
                    }
                }
            } else{
                int endBegPos = midPos;
                int endBegIndex = midIndex;
                while(endBegPos<0){
                    endBegIndex--;
                    if(isTime) endBegPos += sortedPageList.get(endBegIndex).getPageTimeLen();
                    else endBegPos += sortedPageList.get(endBegIndex).getPageValueLen();
                }
                dataExchange(isTime, begIndex, begPos, endBegIndex, endBegPos, mid-beg);
                begPos = midPos;
                begIndex = midIndex;
                midPos += (mid-beg);
                int upBoundLen = 0;
                while(true) {
                    if(isTime) upBoundLen = sortedPageList.get(midIndex).getPageTimeLen();
                    else upBoundLen = sortedPageList.get(midIndex).getPageValueLen();
                    if(midPos >= upBoundLen) {
                        midPos -= upBoundLen;
                        midIndex++;
                    } else {
                        break;
                    }
                }
                int t = beg;
                beg = mid;
                mid = 2*mid-t;
            }
        }
    }

    public void dataExchange(boolean isTime, int begIndex, int begPos, int endBegIndex, int endBegPos, int len) {
        if(dataMoveBegIndex != begIndex) {
            dataMoveBegIndex = begIndex;
            if(isTime) dataMoveBegVals = sortedPageList.get(begIndex).getTimeData().vals;
            else dataMoveBegVals = sortedPageList.get(begIndex).getValueData().vals;
        }
        if(dataMoveEndIndex != endBegIndex) {
            dataMoveEndIndex = endBegIndex;
            if(isTime) dataMoveEndVals = sortedPageList.get(endBegIndex).getTimeData().vals;
            else dataMoveEndVals = sortedPageList.get(endBegIndex).getValueData().vals;
        }
        for (int i=0; i<len; i++) {
            dataMoveBegVals[begPos] = (byte) (dataMoveBegVals[begPos] ^ dataMoveEndVals[endBegPos]);
            dataMoveEndVals[endBegPos] = (byte) (dataMoveBegVals[begPos] ^ dataMoveEndVals[endBegPos]);
            dataMoveBegVals[begPos] = (byte) (dataMoveBegVals[begPos] ^ dataMoveEndVals[endBegPos]);
            begPos++;
            endBegPos++;
            if(begPos>=dataMoveBegVals.length){
                begIndex++;
                dataMoveBegIndex = begIndex;
                begPos = 0;
                if(isTime){
                    dataMoveBegVals = sortedPageList.get(begIndex).getTimeData().vals;
                } else{
                    dataMoveBegVals = sortedPageList.get(begIndex).getValueData().vals;
                }
            }
            if(endBegPos>=dataMoveEndVals.length && endBegIndex<sortedPageList.size()-1){
                endBegIndex++;
                endBegPos = 0;
                dataMoveEndIndex = endBegIndex;
                if(isTime){
                    dataMoveEndVals = sortedPageList.get(endBegIndex).getTimeData().vals;
                } else{
                    dataMoveEndVals = sortedPageList.get(endBegIndex).getValueData().vals;
                }
            }
        }
    }

    public void lenDataMove(boolean isTime, int begIndex, int begPos, int midIndex, int midPos, int endIndex, int endPos) {   // 把mid到end之间的元素移动到beg之前
        dataMoveEndIndex = -1;
        dataMoveBegIndex = -1;
        int beg = begPos;
        int mid = midPos;
        for(int i=begIndex; i<midIndex;i++) {
            mid += sortedPageList.get(i).getCount();
        }
        int end = endPos;
        for(int i=begIndex; i<endIndex;i++) {
            end += sortedPageList.get(i).getCount();
        }
        if(mid == end || mid == beg) return;
        while(mid>beg && end>mid) {
            if(end-mid <= mid-beg) {
                int endBegPos = endPos - (end-mid);
                int endBegIndex = endIndex;
                while(endBegPos<0){
                    endBegIndex--;
                    endBegPos += sortedPageList.get(endBegIndex).getCount();
                }
                lenDataExchange(isTime, begIndex, begPos, endBegIndex, endBegPos, end-mid);
                beg = beg+end-mid;
                begPos += end-mid;
                int upBoundLen = 0;
                while(true) {
                    upBoundLen = sortedPageList.get(begIndex).getCount();
                    if(begPos >= upBoundLen) {
                        begPos -= upBoundLen;
                        begIndex++;
                    } else {
                        break;
                    }
                }
            } else{
                int endBegPos = midPos;
                int endBegIndex = midIndex;
                while(endBegPos<0){
                    endBegIndex--;
                    endBegPos += sortedPageList.get(endBegIndex).getCount();
                }
                lenDataExchange(isTime, begIndex, begPos, endBegIndex, endBegPos, mid-beg);
                begPos = midPos;
                begIndex = midIndex;
                midPos += (mid-beg);
                int upBoundLen = 0;
                while(true) {
                    upBoundLen = sortedPageList.get(midIndex).getCount();
                    if(midPos >= upBoundLen) {
                        midPos -= upBoundLen;
                        midIndex++;
                    } else {
                        break;
                    }
                }
                int t = beg;
                beg = mid;
                mid = 2*mid-t;
            }
        }
    }

    public void lenDataExchange(boolean isTime, int begIndex, int begPos, int endBegIndex, int endBegPos, int len) {
        if(dataMoveBegIndex != begIndex) {
            dataMoveBegIndex = begIndex;
            if(isTime) dataMoveBegVals = sortedPageList.get(begIndex).getTimeData().lens;
            else dataMoveBegVals = sortedPageList.get(begIndex).getValueData().lens;
            dataMoveBegSize = sortedPageList.get(begIndex).getCount();
        }
        if(dataMoveEndIndex != endBegIndex) {
            dataMoveEndIndex = endBegIndex;
            if(isTime) dataMoveEndVals = sortedPageList.get(endBegIndex).getTimeData().lens;
            else dataMoveEndVals = sortedPageList.get(endBegIndex).getValueData().lens;
            dataMoveEndSize = sortedPageList.get(endBegIndex).getCount();
        }
        for (int i=0; i<len; i++) {
            lenDataExchange(begPos, endBegPos);
            begPos++;
            endBegPos++;
            if(begPos>=dataMoveBegSize){
                begIndex++;
                begPos = 0;
                dataMoveBegIndex = begIndex;
                dataMoveBegSize = sortedPageList.get(begIndex).getCount();
                if(isTime){
                    dataMoveBegVals = sortedPageList.get(begIndex).getTimeData().lens;
                } else{
                    dataMoveBegVals = sortedPageList.get(begIndex).getValueData().lens;
                }
            }
            if(endBegPos>=dataMoveEndSize && endBegIndex<sortedPageList.size()-1){
                endBegIndex++;
                endBegPos = 0;
                dataMoveEndIndex = endBegIndex;
                dataMoveEndSize = sortedPageList.get(endBegIndex).getCount();
                if(isTime){
                    dataMoveEndVals = sortedPageList.get(endBegIndex).getTimeData().lens;
                } else{
                    dataMoveEndVals = sortedPageList.get(endBegIndex).getValueData().lens;
                }
            }
        }
    }

    public void lenDataExchange(int pos1, int pos2) {
        // 位置为pos1和pos2两个位置的元素
        byte unmask1 = (byte) (0x3 << (2*(3-pos1%4)));
        byte unmask2 = (byte) (0x3 << (2*(3-pos2%4)));
        byte val1 = (byte) ((dataMoveBegVals[pos1/4]&unmask1)>>(2*(3-pos1%4)));
        byte val2 = (byte) ((dataMoveEndVals[pos2/4]&unmask2)>>(2*(3-pos2%4)));
        val1 = (byte) (val1 & 0x3);
        val2 = (byte) (val2 & 0x3);
        dataMoveBegVals[pos1/4] = (byte) ((dataMoveBegVals[pos1/4]&(~unmask1))|(val2<<(2*(3-pos1%4))));
        dataMoveEndVals[pos2/4] = (byte) ((dataMoveEndVals[pos2/4]&(~unmask2))|(val1<<(2*(3-pos2%4))));
    }

    public void dataMove(byte[] data, int beg, int mid, int end) {
        // 非递归版本
        if(mid == end || mid == beg) return;
        while(mid>beg && end>mid) {
            if(end-mid <= mid-beg) {
                dataExchange(data, beg, end, end-mid);
                beg = beg+end-mid;
            } else{
                dataExchange(data, beg, 2*mid-beg, mid-beg);
                int t = beg;
                beg = mid;
                mid = 2*mid-t;
            }
        }
    }

    public void dataExchange(byte[] data, int beg, int end, int len) {
        for (int i=0; i<len; i++) {
            data[beg+i] = (byte) (data[beg+i] ^ data[end-len+i]);
            data[end-len+i] = (byte) (data[beg+i] ^ data[end-len+i]);
            data[beg+i] = (byte) (data[beg+i] ^ data[end-len+i]);
        }
    }

    public void lenDataExchange(byte[] lens, int pos1, int pos2) {
        // 交换len数组中位置为pos1和pos2两个位置的元素
        byte unmask1 = (byte) (0x3 << (2*(3-pos1%4)));
        byte unmask2 = (byte) (0x3 << (2*(3-pos2%4)));
        byte val1 = (byte) ((lens[pos1/4]&unmask1)>>(2*(3-pos1%4)));
        byte val2 = (byte) ((lens[pos2/4]&unmask2)>>(2*(3-pos2%4)));
        val1 = (byte) (val1 & 0x3);
        val2 = (byte) (val2 & 0x3);
        lens[pos1/4] = (byte) ((lens[pos1/4]&(~unmask1))|(val2<<(2*(3-pos1%4))));
        lens[pos2/4] = (byte) ((lens[pos2/4]&(~unmask2))|(val1<<(2*(3-pos2%4))));
    }

    public void lenDataExchange(byte[] lens, int beg, int end, int len) {
        for (int i=0; i<len; i++) {
            lenDataExchange(lens, beg+i, end-len+i);
        }
    }


    public void lenDataMove(byte[] lens, int begInd, int midInd, int endInd) {
        if(midInd == endInd || midInd == begInd) return;
        while(midInd>begInd && endInd>midInd) {
            if(endInd-midInd <= midInd-begInd) {
                lenDataExchange(lens, begInd, endInd, endInd-midInd);
                begInd = begInd+endInd-midInd;
            } else{
                lenDataExchange(lens, begInd, 2*midInd-begInd, midInd-begInd);
                int t = begInd;
                begInd = midInd;
                midInd = 2*midInd-t;
            }
        }
    }

}
