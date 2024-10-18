package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.tsfile.encoding.decoder.*;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class CompressedTVList extends Series{  //change!
    // 先完全不添加统计信息
    protected TSDataType dataType;
    private Encoder timeEncoder;
    private PublicBAOS timeOut;
    private Encoder valueEncoder;
    private PublicBAOS valueOut;
    protected Decoder valueDecoder;
    protected Decoder timeDecoder;

    protected long totalDatalen = 0; // 按照bit记录数据的长度
    private Statistics<? extends Serializable> statistics;  // todo:直接从这里返回总点数

    //耗时具体统计
    //长序列的实验
    //数据类型的实验
    public CompressedTVList(TSDataType dataType) {//todo:根据不同的数据类型和配置设置不同的encode和decode
        this.dataType = dataType;
        this.statistics = Statistics.getStatsByType(dataType);
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();
        this.timeEncoder = new DeltaDeltaLongEncoder();  // 新实现的二阶差分算法
        //this.timeEncoder = new DeltaGorillaEncoder();
        // this.timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        this.valueEncoder = new DoublePrecisionEncoderV2();  // gorilla编码
    }

    public CompressedTVList(TSDataType dataType, TVList tvList) {//todo:根据不同的数据类型和配置设置不同的encode和decode
        this.dataType = dataType;
        this.statistics = Statistics.getStatsByType(dataType);
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();
        this.timeEncoder = new DeltaDeltaLongEncoder();  // 新实现的二阶差分算法
        // this.timeEncoder = new DeltaGorillaEncoder();
        //this.timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        this.valueEncoder = new DoublePrecisionEncoderV2();  // gorilla编码
        for(int i=0; i<tvList.rowCount(); i++){
            putDouble(tvList.getTime(i), tvList.getDouble(i));
        }
        tvList.clear();
    }

    @Override
    public PublicBAOS getTimeOut() {
        return timeOut;
    }

    @Override
    public PublicBAOS getValueOut() {
        return valueOut;
    }

    @Override
    public Statistics<? extends Serializable> getStatistics() {
        return statistics;
    }

    @Override
    public boolean isSorted() {
        return sorted;
    }

    @Override
    public void sort() {

    }

    @Override
    public void increaseReferenceCount() {

    }

    @Override
    public int getReferenceCount() {
        return 0;
    }

    @Override
    public int rowCount() {
        return statistics.getCount2();
    }

    @Override
    public long getTime(int index) {
        return 0;
    }

    public void putDouble(long time, double value) {
        timeEncoder.encode(time, timeOut);
        valueEncoder.encode(value, valueOut);
        appendNumberToCSV(8-timeEncoder.getBitsleft()+8*timeOut.size());
        appendNumberToCSV(8-valueEncoder.getBitsleft()+8*valueOut.size());
        statistics.update(time, value);
        if (sorted && statistics.getCount() > 1 && time < statistics.getEndTime()) {
            sorted = false;  // 如果当前输入的时间比之前时间的最大值还小，那么说明时间戳一定是乱序的
        }
    }

    public void putDoubles(long[] time, double[] value, BitMap bitMap, int start, int end) {
        // 简化版实现
        for(int i=start; i<end; i++){
            putDouble(time[i], value[i]);
        }
    }

    public String toString(){
        return this.valueOut.toString()+"!!!!";
    }

    @Override
    public TVList getTvListByColumnIndex(List<Integer> columnIndexList, List<TSDataType> dataTypeList) {
        return null;
    }

    @Override
    public int getValueIndex(int index) {
        return 0;
    }

    @Override
    public long getMaxTime() {
        return 0;
    }

    @Override
    public TVList clone() {
        return null;
    }

    @Override
    protected void set(int src, int dest) {

    }

    @Override
    protected void expandValues() {

    }

    @Override
    protected void releaseLastValueArray() {

    }

    @Override
    protected void releaseLastTimeArray() {

    }

    @Override
    public int delete(long lowerBound, long upperBound) {
        return 0;
    }

    @Override
    protected void cloneAs(TVList cloneList) {

    }

    @Override
    public void clear() {

    }

    @Override
    void clearValue() {

    }

    @Override
    protected void clearTime() {

    }

    @Override
    protected void checkExpansion() {

    }

    @Override
    protected Object getPrimitiveArraysByType(TSDataType dataType) {
        return null;
    }

    @Override
    protected long[] cloneTime(long[] array) {
        return new long[0];
    }

    @Override
    public TimeValuePair getTimeValuePair(int index) {
        return null;
    }

    @Override
    protected TimeValuePair getTimeValuePair(int index, long time, Integer floatPrecision, TSEncoding encoding) {
        return null;
    }

    @Override
    protected void writeValidValuesIntoTsBlock(TsBlockBuilder builder, int floatPrecision, TSEncoding encoding, List<TimeRange> deletionList) {

    }

    @Override
    public TSDataType getDataType() {
        return null;
    }

    public TVList convert() throws IOException{  // 将compressedTVList转换成普通的TVList
//        try {
//            // timeEncoder.flush(timeOut);
//            // valueEncoder.flush(valueOut);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
        TVList tvList =  TVList.newList(dataType);
        timeDecoder = new DeltaDeltaLongDecoder();
        valueDecoder = new DoublePrecisionDecoderV2();
        timeDecoder.reset();
        // 将ByteArrayOutputStream类型的变量转变成ByteBuffer类型
        // 共享底层字节数组
        byte[] timeByteArray = timeOut.getBuf();
        byte[] valueByteArray = valueOut.getBuf();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);
        int dataCount = statistics.getCount2();
        while (dataCount>0) {
            try {
                if (!timeDecoder.hasNext(timeBuffer)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long timestamp = timeDecoder.readLong(timeBuffer);
            double value = valueDecoder.readDouble(valueBuffer);
            tvList.putDouble(timestamp, value);
            dataCount--;
        }

        //System.out.println(tvList.rowCount);//change!
        return tvList;
    }

    public void all_uncompressing_sort() throws IOException {
        // 全部数据解压缩到tvlist中，在tvlist中进行排序，将排序后的数据再重新编码回压缩表示当中
        try {
            timeEncoder.flush(timeOut);
            valueEncoder.flush(valueOut);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TVList listTemp = convert();
        listTemp.sort();
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();
        this.timeEncoder = new DeltaDeltaLongEncoder();  // 新实现的二阶差分算法
        //this.timeEncoder = new DeltaGorillaEncoder();
        // this.timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        this.valueEncoder = new DoublePrecisionEncoderV2();
        for(int i=0;i<listTemp.rowCount;i++){
            putDouble(listTemp.getTime(i), listTemp.getDouble(i));
        }
        return;
    }

    public void disordered_uncompressing_sort(int l) throws IOException{
        boolean isOrdered = disordered_uncompressing_sort_ope(l);
        while (!isOrdered){
            isOrdered = disordered_uncompressing_sort_ope(l);
        }
        return;
    }

    public boolean disordered_uncompressing_sort_ope(int l) throws IOException {
        // 乱序数据解压排序的一次基本操作
        //  返回乱序点是否有l个，如果乱序点小于l个，认为经过这次基本操作后，序列变成有序的
        try {
            timeEncoder.flush(timeOut);
            valueEncoder.flush(valueOut);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TVList OOOtvList =  TVList.newList(dataType); // out-of-order tvlist
        // timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        timeDecoder = new DeltaDeltaLongDecoder();
        timeDecoder.reset();
        valueDecoder = new DoublePrecisionDecoderV2();
        byte[] timeByteArray = timeOut.toByteArray();
        byte[] valueByteArray = valueOut.toByteArray();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);

        // 将数据分成有序区和无序区
        long maxTimestamp = 0;
        int OOOsize = 0;

        while (OOOsize < l) {
            try {
                if (!timeDecoder.hasNext(timeBuffer)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long timestamp = timeDecoder.readLong(timeBuffer);
            double value = valueDecoder.readDouble(valueBuffer);
            if (timestamp < maxTimestamp) {
                OOOtvList.putDouble(timestamp, value);
                OOOsize++;
            } else{
                maxTimestamp = timestamp;
            }
        }
        OOOtvList.putDouble(Long.MAX_VALUE, 0);

        // tvlistsize = RamUsageEstimator.sizeOf(this);
        // tvlistsize = RamUsageEstimator.sizeOf(OOOtvList);

        // 无序区tvlist排序，使用原本方法
        OOOtvList.sort();
        boolean isordered;
        if(OOOsize == l){
            isordered = false;
        } else {
            isordered = true;
        }

        // 有序区无序区合并到压缩表示中
        PublicBAOS orderTimeOut = new PublicBAOS();
        PublicBAOS orderValueOut = new PublicBAOS();
        //timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        timeDecoder = new DeltaDeltaLongDecoder();
        this.timeDecoder.reset();
        valueDecoder = new DoublePrecisionDecoderV2();
        valueDecoder.reset();
        timeByteArray = timeOut.toByteArray();
        valueByteArray = valueOut.toByteArray();
        timeBuffer = ByteBuffer.wrap(timeByteArray);
        valueBuffer = ByteBuffer.wrap(valueByteArray);
        long maxOrderTimestamp = 0;
        long orderTime = 0;
        double orderValue = 0;
        if (timeDecoder.hasNext(timeBuffer)) {
            orderTime = timeDecoder.readLong(timeBuffer);
            orderValue = valueDecoder.readDouble(valueBuffer);
        }
        int OOO_index = 0;

        for(int i=0; i<rowCount();i++) {
            if(orderTime<maxOrderTimestamp && OOOsize>0){
                // 跳过乱序部分
                if (timeDecoder.hasNext(timeBuffer)) {
                    orderTime = timeDecoder.readLong(timeBuffer);
                    orderValue = valueDecoder.readDouble(valueBuffer);
                }
                i--;
                OOOsize--;
                continue;
            }
            if(orderTime<=OOOtvList.getTime(OOO_index)){
                maxOrderTimestamp = orderTime;
                timeEncoder.encode(orderTime, orderTimeOut);
                valueEncoder.encode(orderValue, orderValueOut);
                if (timeDecoder.hasNext(timeBuffer)) {
                    orderTime = timeDecoder.readLong(timeBuffer);
                    orderValue = valueDecoder.readDouble(valueBuffer);
                } else{
                    orderTime = Long.MAX_VALUE;  // 数据读完之后，将其设置成最大值
                }
            } else{
                timeEncoder.encode(OOOtvList.getTime(OOO_index), orderTimeOut);
                valueEncoder.encode(OOOtvList.getDouble(OOO_index), orderValueOut);
                OOO_index++;
            }
        }
        this.timeOut = orderTimeOut;
        this.valueOut = orderValueOut;

        return isordered;
    }

    public void convertAndSort() throws IOException {
        // 基本实现：在这里数据放入顺序区是靠一步压缩实现的
        // 按照顺乱序分离的方法，将数据分到有序区（压缩形式）和无序区（普通tvlist）
        // 在普通tvlist中排序
        // 将两个有序部分合并
        // 这里是按方案一的方法实现的
        //long tvlistsize = RamUsageEstimator.sizeOf(this);
        try {
            timeEncoder.flush(timeOut);
            valueEncoder.flush(valueOut);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TVList OOOtvList =  TVList.newList(dataType); // out-of-order tvlist
        PublicBAOS orderTimeOut = new PublicBAOS();
        PublicBAOS orderValueOut = new PublicBAOS();
        timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        // timeDecoder = new DeltaDeltaLongDecoder();
        // timeDecoder = new DeltaGorillaDecoder();
        valueDecoder = new DoublePrecisionDecoderV2();
        byte[] timeByteArray = timeOut.toByteArray();
        byte[] valueByteArray = valueOut.toByteArray();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);

        // 将数据分成有序区和无序区
        long maxTimestamp = 0;
        boolean isLastOOO = false;
        while (true) {
            try {
                if (!timeDecoder.hasNext(timeBuffer)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long timestamp = timeDecoder.readLong(timeBuffer);
            double value = valueDecoder.readDouble(valueBuffer);
            if (timestamp > maxTimestamp) {
                maxTimestamp = timestamp;
                if (true) {  //先全部重新编码
                    // 当isLastOOO为真时，重新编码
                    timeEncoder.encode(timestamp, orderTimeOut);
                    valueEncoder.encode(value, orderValueOut);
                }
//                } else {
//                    // 借用原本编码，需要检查原本的编码是不是长度符合要求
//                    timeEncoder.simpleEncode(timestamp, orderTimeOut, bitnum, bits);
//                    valueEncoder.simpleEncode(value, orderValueOut, bitnum, bits);
//                }
                isLastOOO = false;
            }
            else {
                OOOtvList.putDouble(timestamp, value);
                isLastOOO = true;
            }
        }
        OOOtvList.putDouble(Long.MAX_VALUE, 0);
        this.timeEncoder.flush(orderTimeOut);
        this.valueEncoder.flush(orderValueOut);
//        this.timeOut.reset();
////        this.timeOut = null;
////        this.valueOut = null;
//        this.valueOut.reset();
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();

        //tvlistsize = RamUsageEstimator.sizeOf(this);
        //tvlistsize = RamUsageEstimator.sizeOf(OOOtvList);

        // 无序区tvlist排序，使用原本方法
        OOOtvList.sort();

        // 有序区无序区合并到压缩表示中
        this.timeDecoder.reset();
        this.timeDecoder.reset();
        timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        valueDecoder = new DoublePrecisionDecoderV2();
        byte[] orderTimeByteArray = orderTimeOut.toByteArray();
        byte[] orderValueByteArray = orderValueOut.toByteArray();
        ByteBuffer orderTimeBuffer = ByteBuffer.wrap(orderTimeByteArray);
        ByteBuffer orderValueBuffer = ByteBuffer.wrap(orderValueByteArray);
        long orderTime = 0;
        double orderValue = 0;
        if (timeDecoder.hasNext(orderTimeBuffer)) {
               orderTime = timeDecoder.readLong(orderTimeBuffer);
               orderValue = valueDecoder.readDouble(orderValueBuffer);
        }
        int OOO_index = 0;

        for(int i=0; i<rowCount();i++) {
            if(orderTime<=OOOtvList.getTime(OOO_index)){
                timeEncoder.encode(orderTime, timeOut);
                valueEncoder.encode(orderValue, valueOut);
                if (timeDecoder.hasNext(orderTimeBuffer)) {
                    orderTime = timeDecoder.readLong(orderTimeBuffer);
                    orderValue = valueDecoder.readDouble(orderValueBuffer);
                } else{
                    orderTime = Long.MAX_VALUE;  // 数据读完之后，将其设置成最大值
                }
            } else{
                timeEncoder.encode(OOOtvList.getTime(OOO_index), timeOut);
                valueEncoder.encode(OOOtvList.getDouble(OOO_index), valueOut);
                OOO_index++;
            }
        }
        //tvlistsize = RamUsageEstimator.sizeOf(this);
        //tvlistsize++;
        return;
    }

    public void quickConvertAndSort() throws IOException {
        // 使用的方案一的方法
        // 利用字节流的copy来减少一次编码的用时
        // 按照顺乱序分离的方法，将数据分到有序区（压缩形式）和无序区（普通tvlist）
        // 在普通tvlist中排序
        // 将两个有序部分合并
        try {
            timeEncoder.flush(timeOut);
            valueEncoder.flush(valueOut);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TVList OOOtvList =  TVList.newList(dataType); // out-of-order tvlist
        PublicBAOS orderTimeOut = new PublicBAOS();
        PublicBAOS orderValueOut = new PublicBAOS();
        timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        valueDecoder = new DoublePrecisionDecoderV2();
        byte[] timeByteArray = timeOut.toByteArray();
        byte[] valueByteArray = valueOut.toByteArray();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);

        // 将数据分成有序区和无序区
        long maxTimestamp = 0;
        boolean isLastOOO = false;
        boolean isLastLastOOO = false;
        while (true) {
            try {
                if (!timeDecoder.hasNext(timeBuffer)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long timestamp = timeDecoder.readLong(timeBuffer);
            double value = valueDecoder.readDouble(valueBuffer);
            if (timestamp > maxTimestamp) {  // 写入顺序的压缩表示区
                // 对于时间戳列，只有当在这个之前的两个值均是顺序时，才可以加速写入
                // 对于值列，如果这个值之前的那个值是顺序的，就可以通过加速方式写入
                maxTimestamp = timestamp;
                if (isLastOOO) {  //先全部重新编码
                    // 当isLastOOO为真时，重新编码
                    timeEncoder.encode(timestamp, orderTimeOut);
                    valueEncoder.encode(value, orderValueOut);
                }
                else {
                    // 借用原本编码，需要检查原本的编码是不是长度符合要求
//                    valueEncoder.simpleEncode(value, orderValueOut, bitnum, bits);
//                    if(isLastLastOOO){
//                        timeEncoder.encode(value, orderValueOut);
//                    } else {
//                        timeEncoder.simpleEncode(timestamp, orderTimeOut, bitnum, bits);
//                    }
                }
                isLastOOO = false;
            }
            else {
                OOOtvList.putDouble(timestamp, value);
                isLastOOO = true;
            }
        }
        OOOtvList.putDouble(Long.MAX_VALUE, 0);
        this.timeEncoder.flush(orderTimeOut);
        this.valueEncoder.flush(orderValueOut);
        this.timeOut.reset();
        this.valueOut.reset();

        // 无序区tvlist排序，使用原本方法
        OOOtvList.sort();

        // 有序区无序区合并到压缩表示中
        this.timeDecoder.reset();
        this.timeDecoder.reset();
        timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        valueDecoder = new DoublePrecisionDecoderV2();
        byte[] orderTimeByteArray = orderTimeOut.toByteArray();
        byte[] orderValueByteArray = orderValueOut.toByteArray();
        ByteBuffer orderTimeBuffer = ByteBuffer.wrap(orderTimeByteArray);
        ByteBuffer orderValueBuffer = ByteBuffer.wrap(orderValueByteArray);
        long orderTime = 0;
        double orderValue = 0;
        if (timeDecoder.hasNext(orderTimeBuffer)) {
            orderTime = timeDecoder.readLong(orderTimeBuffer);
            orderValue = valueDecoder.readDouble(orderValueBuffer);
        }
        int OOO_index = 0;

        for(int i=0; i<rowCount();i++) {
            if(i==65){
                i=65;
            }
            if(orderTime<=OOOtvList.getTime(OOO_index)){
                timeEncoder.encode(orderTime, timeOut);
                valueEncoder.encode(orderValue, valueOut);
                if (timeDecoder.hasNext(orderTimeBuffer)) {
                    orderTime = timeDecoder.readLong(orderTimeBuffer);
                    orderValue = valueDecoder.readDouble(orderValueBuffer);
                } else{
                    orderTime = Long.MAX_VALUE;  // 数据读完之后，将其设置成最大值
                }
            } else{
                timeEncoder.encode(OOOtvList.getTime(OOO_index), timeOut);
                valueEncoder.encode(OOOtvList.getDouble(OOO_index), valueOut);
                OOO_index++;
            }
        }
        return;
    }

    public void convertAndSort2() throws IOException {
        // 按照顺乱序分离的方法，将数据分到有序区（压缩形式）和无序区（普通tvlist）
        // 顺序区的数据不变，而是利用一个bit来表示是否是顺序数据
        // 在普通tvlist中排序
        // 将两个有序部分合并
        // 这里是按方案二的方法实现的
        // long tvlistsize = RamUsageEstimator.sizeOf(this);
        try {
            timeEncoder.flush(timeOut);
            valueEncoder.flush(valueOut);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        TVList OOOtvList =  TVList.newList(dataType); // out-of-order tvlist
        // timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        timeDecoder = new DeltaDeltaLongDecoder();
        timeDecoder.reset();
        valueDecoder = new DoublePrecisionDecoderV2();
        byte[] timeByteArray = timeOut.toByteArray();
        byte[] valueByteArray = valueOut.toByteArray();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);

        // 将数据分成有序区和无序区
        long maxTimestamp = 0;
        boolean isLastOOO = false;
        while (true) {
            try {
                if (!timeDecoder.hasNext(timeBuffer)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long timestamp = timeDecoder.readLong(timeBuffer);
            double value = valueDecoder.readDouble(valueBuffer);
            if (timestamp < maxTimestamp) {
                OOOtvList.putDouble(timestamp, value);
            } else{
                maxTimestamp = timestamp;
            }
        }
        OOOtvList.putDouble(Long.MAX_VALUE, 0);

        // tvlistsize = RamUsageEstimator.sizeOf(this);
        // tvlistsize = RamUsageEstimator.sizeOf(OOOtvList);

        // 无序区tvlist排序，使用原本方法
        OOOtvList.sort();

        // 有序区无序区合并到压缩表示中
        PublicBAOS orderTimeOut = new PublicBAOS();
        PublicBAOS orderValueOut = new PublicBAOS();
        //timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        timeDecoder = new DeltaDeltaLongDecoder();
        this.timeDecoder.reset();
        valueDecoder = new DoublePrecisionDecoderV2();
        valueDecoder.reset();
        timeByteArray = timeOut.toByteArray();
        valueByteArray = valueOut.toByteArray();
        timeBuffer = ByteBuffer.wrap(timeByteArray);
        valueBuffer = ByteBuffer.wrap(valueByteArray);
        long maxOrderTimestamp = 0;
        long orderTime = 0;
        double orderValue = 0;
        if (timeDecoder.hasNext(timeBuffer)) {
            orderTime = timeDecoder.readLong(timeBuffer);
            orderValue = valueDecoder.readDouble(valueBuffer);
        }
        int OOO_index = 0;

        for(int i=0; i<rowCount();i++) {
            if(orderTime<maxOrderTimestamp){
                // 跳过乱序部分
                if (timeDecoder.hasNext(timeBuffer)) {
                    orderTime = timeDecoder.readLong(timeBuffer);
                    orderValue = valueDecoder.readDouble(valueBuffer);
                }
                i--;
                continue;
            }
            if(orderTime<=OOOtvList.getTime(OOO_index)){
                maxOrderTimestamp = orderTime;
                timeEncoder.encode(orderTime, orderTimeOut);
                valueEncoder.encode(orderValue, orderValueOut);
                if (timeDecoder.hasNext(timeBuffer)) {
                    orderTime = timeDecoder.readLong(timeBuffer);
                    orderValue = valueDecoder.readDouble(valueBuffer);
                } else{
                    orderTime = Long.MAX_VALUE;  // 数据读完之后，将其设置成最大值
                }
            } else{
                timeEncoder.encode(OOOtvList.getTime(OOO_index), orderTimeOut);
                valueEncoder.encode(OOOtvList.getDouble(OOO_index), orderValueOut);
                OOO_index++;
            }
        }
        this.timeOut = orderTimeOut;
        this.valueOut = orderValueOut;
        // tvlistsize = RamUsageEstimator.sizeOf(this);
        // tvlistsize++;
        // return;
    }

    public void convertAndSort3() throws IOException {
        // 完全的原地排序，使用插入排序的方案
        // 采用O(n2)复杂度的算法
        // long tvlistsize = RamUsageEstimator.sizeOf(this);
        try {
            for(int j=0; j<2; j++) {
                timeEncoder.flush(timeOut);
                valueEncoder.flush(valueOut);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        totalDatalen = 8L *timeOut.size();
        decoderReset();
        byte[] timeByteArray = timeOut.getBuf();
        byte[] valueByteArray = valueOut.getBuf();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);

        // 将数据分成有序区和无序区
        long maxTimestamp = 0;
        while (true) {
            try {
                if (!timeDecoder.hasNext(timeBuffer)) break;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            long timestamp = timeDecoder.readLong(timeBuffer);
            double value = valueDecoder.readDouble(valueBuffer);
            if (timestamp < maxTimestamp) {
                // 乱序数据，通过调用删除和插入的接口，恢复数据的有序性
                //appendToTXT(String.valueOf(timestamp)+"\n");
                //appendToTXT(String.valueOf(maxTimestamp)+"\n");
                compressedDataDelete(timestamp, value);
                //TVList tvList2 = convert();
                compressedDataInsert(timestamp, value);
                //tvList2 = convert();

                maxTimestamp = 0;
                // 恢复decoder，从第一个数据开始decode
                decoderReset();
                timeByteArray = timeOut.getBuf();
                valueByteArray = valueOut.getBuf();
                timeBuffer = ByteBuffer.wrap(timeByteArray);
                valueBuffer = ByteBuffer.wrap(valueByteArray);
                appendToTXT(String.valueOf(totalDatalen));
            } else{
                maxTimestamp = timestamp;
            }
        }

    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {

    }

    @Override
    public int serializedSize() {
        return 0;
    }

    public void decoderReset() {
        // 恢复decoder
        timeDecoder = new DeltaDeltaLongDecoder();
        valueDecoder = new DoublePrecisionDecoderV2();
        this.timeDecoder.reset();
        this.valueDecoder.reset();
    }

    public byte[] rightMoveBuffer(int remain, byte[] buffer, int startPos, int startOffset, int moveLen){
        totalDatalen += moveLen;
        // 从指定位置开始，向右移动指定长度
        // 首先检查buffer的长度，如果不够，则扩容
        // appendToTXT("right move start"+"\n");
        int movePos = (moveLen)/8;
        int moveIndex = moveLen%8;

        if(remain <= movePos+2){
            buffer = expandByteArray(buffer);
        }
        byte nowTail = 0;
        int temp = 0;

        int index = buffer.length-1;
        while (index > startPos){
            temp = buffer[index];
            if(temp<0){
                temp = temp+256;
            }
            nowTail= (byte) (nowTail|temp<<((8-moveIndex)));
            if(index+movePos+1 >= buffer.length){
                index--;
                continue;
                // index=0;
            }
            buffer[index+movePos+1] = nowTail;
            nowTail = (byte) (temp>>>moveIndex);
            index--;
        }

        if(8-startOffset-moveIndex <= 0) { // 8-startOffset+8-moveIndex <= 8
            temp = (byte) (buffer[startPos]<<(8-moveIndex));
            temp = (byte) (temp | nowTail);
            temp = (byte) (temp & ((0x01<<(8-startOffset+8-moveIndex))-1));
            buffer[index+movePos+1] = (byte) temp;
        }
        else {
            temp = buffer[startPos];
            if(temp<0){
                temp = temp+256;
            }
            nowTail= (byte) (nowTail|temp<<(8-moveIndex));
            buffer[startPos+movePos+1] = nowTail;
            nowTail = (byte) (temp>>>moveIndex);
            nowTail = (byte) (nowTail & ((0x01<<(8-startOffset-moveIndex))-1));
            temp = buffer[startPos+movePos];
            temp = (byte) (temp & ~((0x01<<(8-startOffset-moveIndex))-1));
            buffer[startPos+movePos] = (byte) (temp|nowTail);
        }
        // appendToTXT("right move end"+"\n");
        return buffer;
    }

    public void leftMoveBuffer(byte[] buffer, int startPos, int startOffset, int moveLen) {
        totalDatalen -= moveLen;
        // 从指定位置开始，向左移动指定长度
        // 先把最开始的几个左移
        // 再把后面的完整的byte左移
        // appendToTXT("left move start"+"\n");
        int movePos = moveLen/8;
        int moveIndex = moveLen%8;
        int leftStartOffset = (startOffset-moveIndex+8)%8;
        int leftStartPos = -(moveLen-startOffset-8+leftStartOffset)/8+startPos-1;
        byte leftOld = 0;
        byte nowNew = 0;
        int temp = 0;
        temp = buffer[startPos];

        if(8-startOffset<=8-leftStartOffset) {
            // 最左边的几个能放到一个byte里面
            leftOld = (byte) (buffer[leftStartPos] & (-256 >>> leftStartOffset));
            nowNew = (byte) ((buffer[startPos] << (startOffset-leftStartOffset))&((1<<(8-leftStartOffset))-1));
            buffer[leftStartPos] =  (byte)(leftOld | nowNew);
        }
        else{
            leftOld = (byte) (buffer[leftStartPos] & (-256 >>> leftStartOffset));
            nowNew = (byte) ((buffer[startPos] >> (leftStartOffset-startOffset))&((1<<(8-leftStartOffset))-1));
            buffer[leftStartPos] = (byte)(leftOld | nowNew);
            nowNew = (byte) (buffer[startPos] << (8-(leftStartOffset-startOffset)));
            buffer[leftStartPos+1] = nowNew;
        }

        leftStartPos += (leftStartOffset+(8-startOffset))/8;
        leftStartOffset = (leftStartOffset+(8-startOffset))%8;


        int index = startPos+1;
        while (index < buffer.length){
            temp = buffer[index];
            if (temp<0) {
                temp = temp+256;
            }
            leftOld = (byte) (buffer[leftStartPos] & (-256 >>> leftStartOffset));
            nowNew = (byte) (temp >> leftStartOffset);
            buffer[leftStartPos]= (byte) (leftOld | nowNew);
            leftStartPos++;
            if(leftStartOffset != 0) {
                buffer[leftStartPos] = (byte) (temp<<(8-leftStartOffset));
            }
            index++;
        }
        // appendToTXT("left move end"+"\n");
    }

    public void writeBuffer(byte[] buffer, int startPos, int startOffset, int writeLen, byte data[]) {
        // 在buffer的指定位置，写入来自于data的指定长度的元素
        // 要求data的长度要比其实际的容量大至少1
        // appendToTXT("write start"+"\n");
        byte temp = 0;
        byte newdata = 0;
        byte mask = 0;
        // 如果插在一个byte中
        if (startOffset+writeLen<=8){
            temp = buffer[startPos];
            byte mask1 = (byte) ((1<<(8-startOffset-writeLen))-1);
            byte mask2 = (byte) (-256>>>startOffset);
            mask = (byte) (((1<<(8-startOffset-writeLen))-1) | (-256>>>startOffset));
            newdata = (byte) ((data[0]>>startOffset) & (~mask));
            buffer[startPos] = (byte) ((temp&mask)|newdata);
        }
        // 如果插入会跨byte
        else {
            // 写入数据的尾部
            temp = buffer[startPos];
            mask = (byte) (-256>>>startOffset);
            newdata = (byte) ((data[0]>>startOffset) & ((1<<(8-startOffset))-1));
            buffer[startPos] = (byte) ((temp&mask)|newdata);
            // 写入数据的中部
            writeLen -= 8-startOffset;
            int index = 0;
            while (writeLen>=8) {
                temp = (byte) (data[index]<<(8-startOffset));
                index++;
                newdata = (byte) ((data[index]>>startOffset) & ((1<<(8-startOffset))-1));
                buffer[startPos+index] = (byte) (temp | newdata);
                writeLen -= 8;
            }
            // 写入数据的头部
            if (writeLen>0) {
                temp = buffer[startPos + index + 1];
                mask = (byte) ((1 << (8-writeLen))-1);
                newdata = (byte) (data[index]<<(8-startOffset));
                index++;
                newdata |= (byte) ((data[index]>>startOffset) & ((int)(1<<(8-startOffset))-1));
                // newdata |= (byte) ((1<<(8-startOffset))-1);
                buffer[startPos+index] =  (byte) ((temp&mask)|(newdata&(~mask)));
            }
        }
        // appendToTXT("write end"+"\n");
    }



    public ByteBuffer expandBuffer(ByteBuffer buffer) {
        // 给buffer扩容，默认增加32个byte
        int pos = buffer.position();
        int newCapacity = buffer.capacity() + 32;
        byte[] buf = Arrays.copyOf(buffer.array(), newCapacity);
        buffer = ByteBuffer.wrap(buf);
        buffer.position(pos);
        return buffer;
    }

    public byte[] expandByteArray(byte[] buffer) {
        // 给buffer扩容，默认增加32个byte
        // int newCapacity = buffer.length + 32;
        // buffer = Arrays.copyOf(buffer, newCapacity);
        return buffer;
    }

    public void compressedDataDelete(long timestamp, double value) {
        //appendToTXT(String.valueOf(timestamp));
        statistics.setCount(statistics.getCount2()-1);
        // 目前删除操作 要求之后至少还有两个元素
        byte[] timeByteArray = timeOut.getBuf();
        byte[] valueByteArray = valueOut.getBuf();
        Encoder timeEncoderTemp = new DeltaDeltaLongEncoder();  // delta of delta编码
        Encoder valueEncoderTemp = new DoublePrecisionEncoderV2();
        DeltaDeltaLongDecoder timeDecoderTemp = new DeltaDeltaLongDecoder();
        DoublePrecisionDecoderV2 valueDecoderTemp = new DoublePrecisionDecoderV2();
        timeDecoderTemp.reset();
        valueDecoderTemp.reset();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);

        int timeLeftPos;
        int timeLeftOffset;
        int timeRightPos;
        int timeRightOffset;
        int valueLeftPos = 0;
        int valueLeftOffset = 0;
        int valueRightPos = 0;
        int valueRightOffset = 0;
        int valueDataLen;
        int timeDataLen;

        long nextTime;
        long nextNextTime;
        double nextValue;
        double nextNextValue;

        while (true) {
            timeLeftPos = timeBuffer.position()-1;
            timeLeftOffset = 8-timeDecoderTemp.getBitsLeft();
            valueLeftPos = valueRightPos;
            valueLeftOffset = valueRightOffset;
            valueRightPos = valueBuffer.position()-1;
            valueRightOffset = 8-valueDecoderTemp.getBitsLeft();
            if (!timeDecoderTemp.hasNext(timeBuffer)) break;
            long timestampNow = timeDecoderTemp.readLong(timeBuffer);
            double valueNow = valueDecoderTemp.readDouble(valueBuffer);
            if (timestamp != timestampNow) {
                // 不是要删除的数据，跳过
                timeEncoderTemp.encode(timestampNow);
                valueEncoderTemp.encode(valueNow);
            } else{
                // 是要删除的数据，往前再读两个数据，进行编码，然后写回
                // 如果往前的数据不够两个的话，后一个值就和前一个值一样
                if (!timeDecoderTemp.hasNext(timeBuffer)) {
                    nextTime = timestampNow;
                    nextValue = valueNow;
                } else{
                    nextTime = timeDecoderTemp.readLong(timeBuffer);
                    nextValue = valueDecoderTemp.readDouble(valueBuffer);
                }

                valueRightPos = valueBuffer.position()-1;
                valueRightOffset = 8-valueDecoderTemp.getBitsLeft();
                if (!timeDecoderTemp.hasNext(timeBuffer)) {
                    nextNextTime = nextTime;
                    nextNextValue = nextValue;
                } else{
                    nextNextTime = timeDecoderTemp.readLong(timeBuffer);
                    nextNextValue = valueDecoderTemp.readDouble(valueBuffer);
                }
                timeRightPos = timeBuffer.position()-1;
                timeRightOffset = 8-timeDecoderTemp.getBitsLeft();

                // 对新获得的数据进行编码
                PublicBAOS timeOutTemp = new PublicBAOS();
                PublicBAOS valueOutTemp = new PublicBAOS();
                timeEncoderTemp.encode(nextTime, timeOutTemp);
                timeEncoderTemp.encode(nextNextTime, timeOutTemp);
                valueEncoderTemp.encode(nextValue, valueOutTemp);
                valueEncoderTemp.encode(nextNextValue, valueOutTemp);
                valueDataLen = 8*valueOutTemp.size()+(8-valueEncoderTemp.getBitsleft());
                timeDataLen = 8*timeOutTemp.size()+(8-timeEncoderTemp.getBitsleft());
                valueEncoderTemp.flushBuffer(valueOutTemp);
                timeEncoderTemp.flushBuffer(timeOutTemp);
                // 移动原始数组
                ByteBuffer timeBufferTemp = ByteBuffer.wrap(timeByteArray);
                ByteBuffer valueBufferTemp = ByteBuffer.wrap(valueByteArray);
                timeBufferTemp.position(timeByteArray.length-1);
                valueBufferTemp.position(valueByteArray.length-1);
                int lentemp=0;
                lentemp = 8*(timeRightPos-timeLeftPos) + (timeRightOffset-timeLeftOffset);
                if(lentemp > timeDataLen) {
                    leftMoveBuffer(timeByteArray, timeRightPos, timeRightOffset, lentemp-timeDataLen);
                }
                if(lentemp < timeDataLen) {
                    timeByteArray = rightMoveBuffer(timeBufferTemp.remaining(),timeByteArray, timeRightPos, timeRightOffset, timeDataLen-lentemp);
                    timeOut.setBuf(timeByteArray);
                }

                lentemp = 8*(valueRightPos-valueLeftPos) + (valueRightOffset-valueLeftOffset);
                if(lentemp > valueDataLen) {
                    leftMoveBuffer(valueByteArray, valueRightPos, valueRightOffset, lentemp-valueDataLen);
                }
                if(lentemp < valueDataLen) {
                    valueByteArray = rightMoveBuffer( valueBufferTemp.remaining(),valueByteArray, valueRightPos, valueRightOffset, valueDataLen-lentemp);
                    valueOut.setBuf(valueByteArray);
                }
                // 插入数据
                writeBuffer(timeByteArray, timeLeftPos, timeLeftOffset, timeDataLen, timeOutTemp.getBuf());
                writeBuffer(valueByteArray, valueLeftPos, valueLeftOffset, valueDataLen, valueOutTemp.getBuf());
                return;
            }
        }
    }
    public void compressedDataInsert(long timestamp, double value) {
        statistics.setCount(statistics.getCount2()+1);
        // 将数据插到指定位置
        byte[] timeByteArray = timeOut.getBuf();
        byte[] valueByteArray = valueOut.getBuf();
        Encoder timeEncoderTemp = new DeltaDeltaLongEncoder();  // delta of delta编码
        Encoder valueEncoderTemp = new DoublePrecisionEncoderV2();
        DeltaDeltaLongDecoder timeDecoderTemp = new DeltaDeltaLongDecoder();
        DoublePrecisionDecoderV2 valueDecoderTemp = new DoublePrecisionDecoderV2();
        timeDecoderTemp.reset();
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeByteArray);
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueByteArray);

        int timeLeftPos;
        int timeLeftOffset;
        int timeRightPos;
        int timeRightOffset;
        int valueLeftPos;
        int valueLeftOffset;
        int valueRightPos = 0;
        int valueRightOffset = 0;
        int valueDataLen;
        int timeDataLen;

        long nextTime;
        double nextValue;


        // 从前往后遍历，确定元素应该插入的位置
        while (true) {
            timeLeftPos = timeBuffer.position()-1;
            timeLeftOffset = 8-timeDecoderTemp.getBitsLeft();
            valueLeftPos = valueRightPos;
            valueLeftOffset = valueRightOffset;
            valueRightPos = valueBuffer.position()-1;
            valueRightOffset = 8-valueDecoderTemp.getBitsLeft();
            if (!timeDecoderTemp.hasNext(timeBuffer)) break;
            long timestampNow = timeDecoderTemp.readLong(timeBuffer);
            double valueNow = valueDecoderTemp.readDouble(valueBuffer);
            if (timestampNow < timestamp) {
                // 不是要插入的位置
                timeEncoderTemp.encode(timestampNow);
                valueEncoderTemp.encode(valueNow);
            } else{
                // 是要插入的位置，对于时间列，需要再往后读一个元素，数值列不用
                valueRightPos = valueBuffer.position()-1;
                valueRightOffset = 8-valueDecoderTemp.getBitsLeft();
                if (!timeDecoderTemp.hasNext(timeBuffer)) {
                    nextTime = timestampNow;
                    nextValue = valueNow;
                } else {
                    nextTime = timeDecoderTemp.readLong(timeBuffer);
                    nextValue = valueDecoderTemp.readDouble(valueBuffer);
                }
                timeRightPos = timeBuffer.position()-1;
                timeRightOffset = 8-timeDecoderTemp.getBitsLeft();

                // 对新获得的数据进行编码
                // 时间列编码要插入的数据以及之后的两个数据
                // 数值列编码要插入的数据以及之后的一个数据
                PublicBAOS timeOutTemp = new PublicBAOS();
                PublicBAOS valueOutTemp = new PublicBAOS();
                timeEncoderTemp.encode(timestamp, timeOutTemp);
                timeEncoderTemp.encode(timestampNow, timeOutTemp);
                timeEncoderTemp.encode(nextTime, timeOutTemp);
                valueEncoderTemp.encode(value, valueOutTemp);
                valueEncoderTemp.encode(valueNow, valueOutTemp);
                valueEncoderTemp.encode(nextValue, valueOutTemp);
                valueDataLen = 8*valueOutTemp.size()+(8-valueEncoderTemp.getBitsleft());
                timeDataLen = 8*timeOutTemp.size()+(8-timeEncoderTemp.getBitsleft());
                valueEncoderTemp.flushBuffer(valueOutTemp);
                timeEncoderTemp.flushBuffer(timeOutTemp);
                // 移动原始数组
                ByteBuffer timeBufferTemp = ByteBuffer.wrap(timeByteArray);
                ByteBuffer valueBufferTemp = ByteBuffer.wrap(valueByteArray);
                timeBufferTemp.position(timeByteArray.length-1);
                valueBufferTemp.position(valueByteArray.length-1);
                int lentemp;
                lentemp = 8*(timeRightPos-timeLeftPos) + (timeRightOffset-timeLeftOffset);
                if(lentemp > timeDataLen) {
                    leftMoveBuffer(timeByteArray, timeRightPos, timeRightOffset, lentemp-timeDataLen);
                }
                if(lentemp < timeDataLen) {
                    timeByteArray = rightMoveBuffer(timeBufferTemp.remaining(), timeByteArray, timeRightPos, timeRightOffset, timeDataLen-lentemp);

                }

                lentemp = 8*(valueRightPos-valueLeftPos) + (valueRightOffset-valueLeftOffset);
                if(lentemp > valueDataLen) {
                    leftMoveBuffer(valueByteArray, valueRightPos, valueRightOffset, lentemp-valueDataLen);
                }
                if(lentemp < valueDataLen) {
                    valueByteArray = rightMoveBuffer(valueBufferTemp.remaining(), valueByteArray,valueRightPos, valueRightOffset, valueDataLen-lentemp);

                }
                // 插入数据
                writeBuffer(timeByteArray, timeLeftPos, timeLeftOffset, timeDataLen, timeOutTemp.getBuf());
                writeBuffer(valueByteArray, valueLeftPos, valueLeftOffset, valueDataLen, valueOutTemp.getBuf());
                this.timeOut.setBuf(timeByteArray);
                this.valueOut.setBuf(valueByteArray);
                return;
            }
        }
    }

    public static boolean appendToTXT(String content) {
        String filePath = "D:\\senior\\毕设\\画图\\实验章节\\不同排序算法的空间占用情况\\insert_sort.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) { // 第二个参数true表示追加模式
            writer.write(content+"\n");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static boolean appendNumberToCSV(int number) {
        String filePath = "D:\\senior\\毕设\\画图\\SIMD\\delta_len.csv";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            // 将整数转换为字符串，并追加到文件中，每个整数占一行
            writer.write(String.valueOf(number) + "\n");
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

}
