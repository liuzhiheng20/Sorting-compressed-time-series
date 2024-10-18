package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.compressedsort.*;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaDeltaLongDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionDecoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaDeltaLongEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;

public class CompressedLongTVList extends Series {
    protected TSDataType dataType;
    private TS_DELTA_encoder timeEncoder;
    private PublicBAOS timeOut;
    private TS_DELTA_data timeCompressedData;
    private TS_DELTA_data valueCompressedData;
    private V_VARINT_encoder valueEncoder;
    private PublicBAOS valueOut;
    protected V_VARINT_decoder valueDecoder;
    protected TS_DELTA_decoder timeDecoder;

    protected long totalDatalen = 0; // 按照bit记录数据的长度
    private Statistics<? extends Serializable> statistics;  // todo:直接从这里返回总点数

    //耗时具体统计
    //长序列的实验
    //数据类型的实验
    public CompressedLongTVList(TSDataType dataType) {//todo:根据不同的数据类型和配置设置不同的encode和decode
        this.dataType = dataType;
        this.statistics = Statistics.getStatsByType(dataType);
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();
        this.timeCompressedData = new TS_DELTA_data();
        this.valueCompressedData = new TS_DELTA_data();
        this.timeEncoder = new TS_DELTA_encoder();  // 新实现的二阶差分算法
        this.valueEncoder = new V_VARINT_encoder();
    }

    public CompressedLongTVList(TSDataType dataType, TVList tvList) {//todo:根据不同的数据类型和配置设置不同的encode和decode
        this.dataType = dataType;
        this.statistics = Statistics.getStatsByType(dataType);
        this.timeOut = new PublicBAOS();
        this.valueOut = new PublicBAOS();
        this.timeCompressedData = new TS_DELTA_data();
        this.valueCompressedData = new TS_DELTA_data();
        this.timeEncoder = new TS_DELTA_encoder();  // 新实现的二阶差分算法
        this.valueEncoder = new V_VARINT_encoder();
        for(int i=0; i<tvList.rowCount(); i++){
            putDouble(tvList.getTime(i), tvList.getDouble(i));
        }
        tvList.clear();
    }

    @Override
    public PublicBAOS getTimeOut() {
        // 将compressed data中的数据转移到timeOut中
        timeOut.write(timeEncoder.getValsNum());
        timeOut.write(timeCompressedData.lens, 0, (timeEncoder.getValsNum()-1)/4+1);
        timeOut.write(timeCompressedData.vals, 0, timeEncoder.getValsLen());
        return timeOut;
    }

    @Override
    public PublicBAOS getValueOut() {
        // 将compressed data中的数据转移到valueOut中
        valueOut.write(valueEncoder.getValsNum());
        valueOut.write(valueCompressedData.lens, 0, (valueEncoder.getValsNum()-1)/4+1);
        valueOut.write(valueCompressedData.vals, 0, valueEncoder.getValsLen());
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

    public void putLong(long time, long value) {
        timeEncoder.encode(time, timeCompressedData);
        valueEncoder.encode(value, valueCompressedData);
        //appendNumberToCSV(8-timeEncoder.getBitsleft()+8*timeOut.size());
        //appendNumberToCSV(8-valueEncoder.getBitsleft()+8*valueOut.size());
        statistics.update(time, value);
        if (sorted && statistics.getCount() > 1 && time < statistics.getEndTime()) {
            sorted = false;  // 如果当前输入的时间比之前时间的最大值还小，那么说明时间戳一定是乱序的
        }
    }

    public void persistent_compressing_sort() {
        CompressedDataSorter sorter = new CompressedDataSorter(this.timeCompressedData, this.valueCompressedData);
        sorter.blockSort(0,statistics.getCount2()-1, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());
    }

    public void all_uncompressing_sort() {
        // 全部压缩数据解码排序
        TVList tvList = convert();
        tvList.sort();
        this.timeEncoder.reset();
        this.valueEncoder.reset();
        for(int i=0; i<tvList.rowCount; i++){
            timeEncoder.encode(tvList.getTime(i), timeCompressedData);
            valueEncoder.encode(tvList.getLong(i),  valueCompressedData);
        }
    }

    public void putLongs(long[] time, long[] value, BitMap bitMap, int start, int end) {
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

    public TVList convert(){  // 将compressedTVList转换成普通的TVList
        // 从TS_DELTA_data中恢复数据
        TVList tvList =  TVList.newList(dataType);
        timeDecoder = new TS_DELTA_decoder(0,0,0);
        valueDecoder = new V_VARINT_decoder(0,0,0);        //
        int dataCount = statistics.getCount2();
        while (dataCount>0) {
            long timestamp = timeDecoder.forwardDecode(timeCompressedData);
            long value = valueDecoder.forwardDecode(valueCompressedData);
            tvList.putLong(timestamp, value);
            dataCount--;
        }
        //System.out.println(tvList.rowCount);//change!
        return tvList;
    }

    @Override
    public void serializeToWAL(IWALByteBufferView buffer) {

    }

    @Override
    public int serializedSize() {
        return 0;
    }
}
