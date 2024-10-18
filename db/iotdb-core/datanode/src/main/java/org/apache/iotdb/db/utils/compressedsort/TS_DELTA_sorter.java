package org.apache.iotdb.db.utils.compressedsort;

import org.apache.iotdb.tsfile.utils.TS_DELTA_data;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

public class TS_DELTA_sorter implements Runnable {
    private final byte[] vals;
    private final byte[] lens;

    private TS_DELTA_decoder decoderTemp;

    private int blockThreshold = 300;  // 3
    private int dataMoveThreshold = 1000;

    public TS_DELTA_sorter(byte[] vals,byte[] lens) {
        this.vals = vals;
        this.lens = lens;
    }

    @Override
    public void run() {
        // 开始在一个块内进行数据的排序操作
        //array[index] = (int) Thread.currentThread().getId();
        //System.out.println("Thread " + Thread.currentThread().getId() + " updated index " + index + " to " + array[index]);

    }

    public int sort(int begIndex, int endIndex, int begPos, int endPos) {  // 返回endPos的变化情况，用 newEndPos - oldEndPos
        // 试图拆分成两个不相交的子问题
        // 如果没有点或者只有一个点，直接结束
//         long[] temp = {begIndex, endIndex};
//         writeDataToTXT(temp);
        if(endIndex-begIndex <= 1) {
            return 0;  // 不排序，自然长度也没有变化
        }
        // 如果大小小于基本块大小，直接解决
        if(endIndex - begIndex<=blockThreshold){
            return blockSort(begIndex, endIndex, begPos, endPos)-endPos;
        }
        // 遍历，寻找合适的问题拆分
        int midIndex = findBestMidIndex(begIndex, endIndex, begPos, endPos);
        int midPos = endPos;
        long block1stMaxVal = 0;
        long block2ndMinVal = Long.MAX_VALUE;
        // 根据midindex的值来确定midPos、block1stMaxVal、block2ndMinVal的值
        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false,0, begIndex, begPos);
        while (decoder.nowPos<endPos) {
            decoder.forwardDecode(vals, lens);
            if(decoder.nowNum<=midIndex && decoder.nowValue > block1stMaxVal) {
                block1stMaxVal = decoder.nowValue;
            }
            if(decoder.nowNum == midIndex) {
                midPos = decoder.nowPos;
            }
            if(decoder.nowNum>midIndex && decoder.nowValue < block2ndMinVal) {
                block2ndMinVal = decoder.nowValue;
            }
        }

        int block1stVar = sort(begIndex, midIndex, begPos, midPos);
        int block2ndVar = sort(midIndex, endIndex, midPos, endPos);
        TS_DELTA_encoder encoder = new TS_DELTA_encoder();
        TS_DELTA_data compressedData = new TS_DELTA_data(this.vals, this.lens);
        block2ndVar += encoder.changeEncode(midIndex, midPos, block2ndMinVal-block1stMaxVal, compressedData, endPos);
        // 将两部分的数据整理合并
        mergeBlock(midPos+block1stVar, midPos, endPos);
        return block1stVar+block2ndVar;
    }

    public int sort2(int begIndex, int endIndex, int begPos, int endPos) {  // 返回endPos的变化情况，用 newEndPos - oldEndPos
        // 只需要满足拆分点是第二个块的最小值即可，同时也是乱序数据
        if(endIndex-begIndex <= 1) {
            return 0;  // 不排序，自然长度也没有变化
        }
        // 如果大小小于基本块大小，直接解决
        if(endIndex - begIndex<=blockThreshold){
            return blockSort(begIndex, endIndex, begPos, endPos)-endPos;
        }
        // 遍历，寻找合适的问题拆分
        long[] res = new long[4];
        findMidIndexWithPosAndValue(begIndex, endIndex, begPos, endPos, res);
        int midIndex = (int) res[0];
        int midPos = (int) res[1];
        long block1stMaxVal = res[2];
        long block2ndMinVal = res[3];
//        int midIndex = findMidIndex(begIndex, endIndex, begPos, endPos);
        if(midIndex == begIndex)  return blockSort(begIndex, endIndex, begPos, endPos)-endPos;  // 已经是一个有序的子问题
        if(midIndex-begIndex <=blockThreshold || endIndex-midIndex<= blockThreshold) {
            return blockSort(begIndex, endIndex, begPos, endPos)-endPos;
        }
//        int midPos = endPos;
//        long block1stMaxVal = 0;
//        long block2ndMinVal = Long.MAX_VALUE;
//        // 根据midindex的值来确定midPos、block1stMaxVal、block2ndMinVal的值
//        // todo:不需要再重新遍历解码一遍数据？可以在findMidIndex函数中实现
//        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false,0, begIndex, begPos);
//        while (decoder.nowPos<endPos) {
//            decoder.forwardDecode(vals, lens);
//            if(decoder.nowNum<=midIndex && decoder.nowValue > block1stMaxVal) {
//                block1stMaxVal = decoder.nowValue;
//            }
//            if(decoder.nowNum == midIndex) {
//                midPos = decoder.nowPos;
//            }
//            if(decoder.nowNum>midIndex && decoder.nowValue < block2ndMinVal) {
//                block2ndMinVal = decoder.nowValue;
//            }
//        }

        int block1stVar = sort2(begIndex, midIndex, begPos, midPos);
        int block2ndVar = sort2(midIndex, endIndex, midPos, endPos);
        TS_DELTA_encoder encoder = new TS_DELTA_encoder();
        TS_DELTA_data compressedData = new TS_DELTA_data(this.vals, this.lens);
        //block2ndVar += encoder.changeEncode(midIndex, midPos, block2ndMinVal-block1stMaxVal, compressedData, endPos);  // change!
        block2ndVar += encoder.changeEncodeWithFixedLen(midIndex, midPos, block2ndMinVal-block1stMaxVal, 4, compressedData, endPos);
        // 将两部分的数据整理合并
        mergeBlock(midPos+block1stVar, midPos, endPos);
        int blockVar = 0;
        if(block1stMaxVal > block2ndMinVal) {
            blockVar = blockSort(begIndex, endIndex, begPos, endPos+block1stVar+block2ndVar, block1stMaxVal, block2ndMinVal, midIndex, midPos+block1stVar)-(endPos+block1stVar+block2ndVar);
            //blockVar = blockSort(begIndex, endIndex, begPos, endPos+block1stVar+block2ndVar)-(endPos+block1stVar+block2ndVar);
        }
//        long[] temp = {block1stVar, block2ndVar, blockVar};
//        writeDataToTXT(temp);
        return block1stVar+block2ndVar+blockVar;
    }

    public int blockSort(int begIndex, int endIndex, int begPos, int endPos) {
        // 在排序的基本块内部，使用持续压缩排序算法进行排序
        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false,0, begIndex, begPos);
        long nowVal = 0;
        int nowValPos = 0;
        int temValPos;
        long temVal;
        int nowInd = begIndex;
        while (true) {
            temValPos = decoder.nowPos;
            if(temValPos >= endPos) {
                break;
            }
            temVal = decoder.forwardDecode(this.vals, this.lens);
            if(temVal >= nowVal || nowInd==begIndex){
                nowVal = temVal;
                nowValPos = temValPos;
                nowInd++;
                if(nowInd == endIndex){
                    return endPos;
                }
            } else{
                // 找到一个乱序点
                endPos += persistentSortOpeHoldLen(nowVal, nowValPos, temVal, temValPos, decoder, begIndex, endPos);  // change!修改基本模块的运行逻辑
//                TS_DELTA_decoder decoder2 = new TS_DELTA_decoder(0,0,0);
//                long[] times = new long[11];
//                for (int i=0; i<11; i++){
//                    times[i] = decoder2.forwardDecode(vals, lens);
//                }
//                decoder = new TS_DELTA_decoder(false,0, begIndex, begPos);
//                nowVal = 0;
//                nowValPos = 0;
//                nowInd = begIndex;
                decoder = decoderTemp;
                nowVal = decoder.nowValue;
                nowInd = decoder.nowNum;
                nowValPos = decoder.nowPos;
            }
        }
        return endPos;
    }

    public int blockSort(int begIndex, int endIndex, int begPos, int endPos, long block1MaxValue, long block2MinValue, int block2Index, int block2Pos) {
        // 在排序的基本块内部，使用持续压缩排序算法进行排序
        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false,block1MaxValue, block2Index, block2Pos);
        long nowVal = block1MaxValue;
        int nowValPos = block2Pos;
        int temValPos;
        long temVal;
        int nowInd = block2Index;
        while (true) {
            temValPos = decoder.nowPos;
            if(temValPos >= endPos) {
                break;
            }
            temVal = decoder.forwardDecode(this.vals, this.lens);
            if (temVal > block1MaxValue){   // 新增加的结束条件，如果当前值大于第一块的最大值，则一定没有重叠
                break;
            }
            if(temVal >= nowVal || nowInd==begIndex){
                nowVal = temVal;
                nowValPos = temValPos;
                nowInd++;
                if(nowInd == endIndex){
                    return endPos;
                }
            } else{
                // 找到一个乱序点
                endPos += persistentSortOpeHoldLen(nowVal, nowValPos, temVal, temValPos, decoder, begIndex, endPos);  // change!修改基本模块的运行逻辑
//                TS_DELTA_decoder decoder2 = new TS_DELTA_decoder(0,0,0);
//                long[] times = new long[11];
//                for (int i=0; i<11; i++){
//                    times[i] = decoder2.forwardDecode(vals, lens);
//                }
                decoder = decoderTemp;
                nowVal = decoder.nowValue;
                nowInd = decoder.nowNum;
                nowValPos = decoder.nowPos;
            }
        }
        return endPos;
    }

    public int findBestMidIndex(int begIndex, int endIndex, int begPos, int endPos) {
        // 寻找可以将原问题分解为两个不相交子问题的划分
        int midIndex = 0;
        long block1stMaxVal = 0;
        long block2ndMinVal = Long.MAX_VALUE;
        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false, 0, begIndex, begPos);
        // 辅助变量
        boolean[] isBlock1stMaxVal = new boolean[endIndex-begIndex];
        boolean[] isBlock2stMinVal = new boolean[endIndex-begIndex];
        while (decoder.nowPos<endPos) {
            decoder.forwardDecode(vals, lens);
            if(decoder.nowValue > block1stMaxVal) {
                block1stMaxVal = decoder.nowValue;
                isBlock1stMaxVal[decoder.nowNum-begIndex-1] = true;
            }
        }
        while (decoder.nowPos > begPos) {
            if(decoder.nowValue < block2ndMinVal) {
                block2ndMinVal = decoder.nowValue;
                isBlock2stMinVal[decoder.nowNum-begIndex-1] = true;
            }
            decoder.backwardDecode(vals, lens);
        }
        for(int i=0; i<endIndex-begIndex; i++) {
            if(isBlock2stMinVal[i] && isBlock1stMaxVal[i]) {
                if(Math.abs(i-(endIndex-begIndex)/2)<Math.abs(midIndex-(endIndex-begIndex)/2)) {
                    midIndex = i;
                }
            }
        }
        return midIndex+begIndex;
    }

    public int findMidIndex(int begIndex, int endIndex, int begPos, int endPos) {
        // 将原问题分解为两个相交的子问题
        // 具体来说，找一个靠近中间的，既是乱序点，也是之后序列中最小值点的点
        int midIndex = 0;
        long block2ndMinVal = Long.MAX_VALUE;
        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false, 0, begIndex, begPos);
        // 辅助变量
        while (decoder.nowNum<endIndex) {
            decoder.forwardDecode(vals, lens);
        }
        while (decoder.nowPos > begPos) {
            //if(decoder.nowValue < block2ndMinVal) { // 判断当前点是否是最小值点
            if(true) {
                block2ndMinVal = decoder.nowValue;
                decoder.backwardDecode(vals, lens);
                if(decoder.nowValue>block2ndMinVal) { // 判断当前点是否是乱序点
                    if(Math.abs(decoder.nowNum-begIndex-(endIndex-begIndex)/2)<Math.abs(midIndex-(endIndex-begIndex)/2)) {
                        midIndex = decoder.nowNum-begIndex;
                    }
                }
            } else{
                decoder.backwardDecode(vals, lens);
            }
        }
        return midIndex+begIndex;
    }

    public void findMidIndexWithPosAndValue(int begIndex, int endIndex, int begPos, int endPos, long[] out) {
        // out[0] = midIndex  out[1] = midPos  out[2] = block1MaxValue  out[3] = block2MinValue
        int midIndex = 0;
        int midPos = 0;
        long block2ndMinVal = Long.MAX_VALUE;
        long block1stMaxVal = Long.MIN_VALUE;
        long valueTemp;
//        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false, 0, begIndex, begPos);
//        // 辅助变量
//        while (decoder.nowNum < endIndex) {  // todo：是否需要先正向遍历到最后？
//            decoder.forwardDecode(vals, lens);
//        }
        TS_DELTA_decoder decoder = new TS_DELTA_decoder(false, 0, endIndex, endPos);
//        long[] temp = {decoder.nowPos, decoder.nowNum, endPos, endIndex};
//        writeDataToTXT(temp);
        while (decoder.nowPos > begPos) {
            valueTemp = decoder.nowValue;
            if(valueTemp<block2ndMinVal) block2ndMinVal = valueTemp;
            if(valueTemp > block1stMaxVal) block1stMaxVal = valueTemp;
            decoder.backwardDecode(vals, lens);
            if(decoder.nowValue > valueTemp) { // 判断当前点是否是乱序点
                if(Math.abs(decoder.nowNum-begIndex-(endIndex-begIndex)/2)<Math.abs(midIndex-(endIndex-begIndex)/2)) {
                    midIndex = decoder.nowNum-begIndex;
                    midPos = decoder.nowPos;
                    out[3] = block2ndMinVal;
                    block1stMaxVal = Long.MIN_VALUE;
                }
            }
        }
        out[0] = midIndex+begIndex;
        out[1] = midPos;
        out[2] = block1stMaxVal;
    }

    public void mergeBlock(int newBeg, int orgBeg, int endPos) {
        // 将block之间的空值整理
        // 将orgBeg到endPos之间的vals数组的内容向前移动至newBeg开始
        if(newBeg == orgBeg) return;
        for(int i=0; i<endPos-orgBeg; i++) {
            vals[newBeg+i] = vals[orgBeg+i];
        }
    }

    public int persistentSortOpe(long nowValue, int nowValuePos, long valueTemp, int valueTempPos, TS_DELTA_decoder forwardDecoder, int blockBegInd, int blockEndPos) {
        // 持续压缩排序算法的一次基本操作
        // 第一个乱序点是valueTemp,nowValue是乱序点之前的一个点
        long upBound = nowValue;   // 这两个变量是否可以删除？
        int upBoundPos = nowValuePos;
        TS_DELTA_decoder backwardDecoder = new TS_DELTA_decoder(forwardDecoder.nowValue, forwardDecoder.nowNum, forwardDecoder.nowPos);
        while (backwardDecoder.backwardDecode(this.vals, this.lens)>valueTemp) {
            upBound = backwardDecoder.nowValue;
            if(backwardDecoder.nowNum == blockBegInd+1) {  //排序的起点不是最小值
                backwardDecoder.backwardDecode(this.vals, this.lens);
                break;
            }
        }
        upBoundPos = backwardDecoder.nowPos;
        int valueTempInd = forwardDecoder.nowNum-1;
        long tailVal = valueTemp;
        int newHeadPos = forwardDecoder.nowPos;
        while(forwardDecoder.nowValue<upBound) {
            tailVal = forwardDecoder.nowValue;
            newHeadPos = forwardDecoder.nowPos;
            forwardDecoder.forwardDecode(this.vals, this.lens);
            if(newHeadPos >= blockEndPos) {
                break;
            }
        }
        // 直接修改三个位置元素的delta
        TS_DELTA_encoder encoder = new TS_DELTA_encoder();
        TS_DELTA_data compressedData = new TS_DELTA_data(this.vals, this.lens);  //delta值发生变化后，后面相应的指针也会发生变化
        int valueTempVar = encoder.changeEncode(valueTempInd, valueTempPos, valueTemp-backwardDecoder.nowValue, compressedData, blockEndPos);
        int upBoundVar = encoder.changeEncode(backwardDecoder.nowNum, upBoundPos, upBound-tailVal, compressedData, blockEndPos);
        int newHeadVar = 0;
        valueTempPos += upBoundVar;
        if(newHeadPos < blockEndPos) {
            newHeadPos += upBoundVar+valueTempVar;
            newHeadVar = encoder.changeEncode(forwardDecoder.nowNum-1, newHeadPos, forwardDecoder.nowValue-nowValue, compressedData, blockEndPos);
        } else {
            newHeadPos += upBoundVar+valueTempVar;
        }
        // 中间部分的元素整体移动
        dataMove(this.vals, upBoundPos, valueTempPos, newHeadPos);
        lenDataMove(this.lens, backwardDecoder.nowNum, valueTempInd, forwardDecoder.nowNum-1);
        return valueTempVar+upBoundVar+newHeadVar;  // 应该返回经过一次排序基本操作后，block的总体的长度变化
    }

    public int persistentSortOpeHoldLen(long nowValue, int nowValuePos, long valueTemp, int valueTempPos, TS_DELTA_decoder forwardDecoder, int blockBegInd, int blockEndPos) {
        // 长度不变的压缩数据的一次基本操作(减少数据整体移动的开销)
        long upBound = nowValue;
        TS_DELTA_decoder backwardDecoder = new TS_DELTA_decoder(forwardDecoder.nowValue, forwardDecoder.nowNum, forwardDecoder.nowPos);
        while (backwardDecoder.backwardDecode(this.vals, this.lens)>valueTemp) {
            upBound = backwardDecoder.nowValue;
            if(backwardDecoder.nowNum == blockBegInd+1) {  //排序的起点不是最小值
                backwardDecoder.backwardDecode(this.vals, this.lens);
                break;
            }
        }
        int upBoundPos = backwardDecoder.nowPos;
        int valueTempInd = forwardDecoder.nowNum-1;
        long tailVal = valueTemp;
        int newHeadPos = forwardDecoder.nowPos;
        while(forwardDecoder.nowValue<upBound) {
            tailVal = forwardDecoder.nowValue;
            newHeadPos = forwardDecoder.nowPos;
            forwardDecoder.forwardDecode(this.vals, this.lens);
            if(newHeadPos >= blockEndPos) {
                break;
            }
        }
        // 直接修改三个位置元素的delta
        TS_DELTA_encoder encoder = new TS_DELTA_encoder();
        TS_DELTA_data compressedData = new TS_DELTA_data(this.vals, this.lens);  //delta值发生变化后，后面相应的指针也会发生变化
        int valueTempVar = encoder.changeEncodeWithFixedLenByInd(valueTempInd, valueTempPos, valueTemp-backwardDecoder.nowValue, backwardDecoder.nowNum, compressedData, forwardDecoder.nowPos);
        int upBoundVar;
        if(newHeadPos < blockEndPos) {
            upBoundVar = encoder.changeEncodeWithFixedLenByInd(backwardDecoder.nowNum, upBoundPos, upBound - tailVal, forwardDecoder.nowNum - 1, compressedData, forwardDecoder.nowPos);
        } else {
            upBoundVar = encoder.changeEncodeWithFixedLenByInd(backwardDecoder.nowNum, upBoundPos, upBound - tailVal, valueTempInd, compressedData, forwardDecoder.nowPos);
        }
        int newHeadVar = 0;
        valueTempPos += upBoundVar;
        if(newHeadPos < blockEndPos) {
            newHeadPos += upBoundVar+valueTempVar;
            newHeadVar = encoder.changeEncodeWithFixedLen(forwardDecoder.nowNum-1, newHeadPos, forwardDecoder.nowValue-nowValue, 4, compressedData, forwardDecoder.nowPos);
        } else {
            newHeadPos += upBoundVar+valueTempVar;
        }
        // 中间部分的元素整体移动
        dataMove(this.vals, upBoundPos, valueTempPos, newHeadPos);
        lenDataMove(this.lens, backwardDecoder.nowNum, valueTempInd, forwardDecoder.nowNum-1);
        decoderTemp = backwardDecoder;
        return valueTempVar+upBoundVar+newHeadVar;  // 应该返回经过一次排序基本操作后，block的总体的长度变化
    }

    public void dataExchange(byte[] data, int beg, int end, int len) {
        for (int i=0; i<len; i++) {
            data[beg+i] = (byte) (data[beg+i] ^ data[end-len+i]);
            data[end-len+i] = (byte) (data[beg+i] ^ data[end-len+i]);
            data[beg+i] = (byte) (data[beg+i] ^ data[end-len+i]);
        }
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

    public void writeLen(int index, int len, byte[] lens) {
        // 更改压缩数据中，指定位置数据的长度
        byte temp = 0;
        if(len == 1) temp= 1;
        if(len == 2) temp = 2;
        if(len == 4) temp = 3;
        byte unmask = (byte) (0x3 << (2*(3-index%4)));
        lens[index/4] = (byte) (lens[index/4]&(~unmask));
        lens[index/4] = (byte) (lens[index/4]|(temp<<(2*(3-index%4))));
    }

    public int getLen(int index, byte[] lens) {
        // 获得压缩数据中，指定位置数据的长度
        byte unmask = (byte) (0x3 << (2*(3-index%4)));
        byte val = (byte) ((lens[index/4]&unmask)>>(2*(3-index%4)));
        val = (byte) (val & 0x3);
        if(val == 3) return 4;
        return val;
    }

    public void writeDataToTXT(long[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\find_mid_index.txt";
        // 使用 try-with-resources 自动关闭资源
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            // 遍历数组并将每个元素写入文件
            for (long number : data) {
                writer.write(number + ","); // 每个数字后面添加换行符
            }
            writer.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
