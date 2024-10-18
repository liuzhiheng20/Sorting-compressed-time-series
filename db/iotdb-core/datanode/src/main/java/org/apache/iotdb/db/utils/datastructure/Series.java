package org.apache.iotdb.db.utils.datastructure;

import com.sun.org.apache.bcel.internal.generic.RETURN;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.WALEntryValue;
import org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.TimeValuePair;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;

import static org.apache.iotdb.db.storageengine.rescon.memory.PrimitiveArrayManager.ARRAY_SIZE;
import static org.apache.iotdb.db.utils.datastructure.TVList.ERR_DATATYPE_NOT_CONSISTENT;
import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_ARRAY_HEADER;
import static org.apache.iotdb.tsfile.utils.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

public abstract class Series implements WALEntryValue {  //change!
    //protected boolean isCompressed = false;
    //protected int rowCount;
    protected boolean sorted = true;
    public abstract TVList convert() throws IOException;

    public static long tvListArrayMemCost(TSDataType type){
        return 0;
    }

    public PublicBAOS getTimeOut(){
        return null;
    }
    public PublicBAOS getValueOut(){
        return null;
    }
    public Statistics<? extends Serializable> getStatistics(){
        return null;
    }
    public abstract boolean isSorted();

    public abstract void sort();

    public abstract void increaseReferenceCount();

    public abstract int getReferenceCount();

    public abstract int rowCount();

    public String toString() {
        if(this instanceof TVList){
            return "111111";
        } else{
            return "222222";
        }
    }

    public abstract long getTime(int index);
    public void putLong(long time, long value) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putInt(long time, int value) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putFloat(long time, float value) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putDouble(long time, double value) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putBinary(long time, Binary value) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public boolean reachMaxChunkSizeThreshold() {
        return false;
    }

    public void putBoolean(long time, boolean value) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putAlignedValue(long time, Object[] value) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putLongs(long[] time, long[] value, BitMap bitMap, int start, int end) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putInts(long[] time, int[] value, BitMap bitMap, int start, int end) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putFloats(long[] time, float[] value, BitMap bitMap, int start, int end) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putDoubles(long[] time, double[] value, BitMap bitMap, int start, int end) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putBinaries(long[] time, Binary[] value, BitMap bitMap, int start, int end) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putBooleans(long[] time, boolean[] value, BitMap bitMap, int start, int end) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public void putAlignedValues(long[] time, Object[] value, BitMap[] bitMaps, int start, int end) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public long getLong(int index) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public int getInt(int index) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public float getFloat(int index) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public double getDouble(int index) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public Binary getBinary(int index) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public boolean getBoolean(int index) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }

    public Object getAlignedValue(int index) {
        throw new UnsupportedOperationException(ERR_DATATYPE_NOT_CONSISTENT);
    }
    public abstract TVList getTvListByColumnIndex(
            List<Integer> columnIndexList, List<TSDataType> dataTypeList);

    public abstract int getValueIndex(int index);

    public abstract long getMaxTime();

    public abstract TVList clone();

    protected abstract void set(int src, int dest);

    protected abstract void expandValues();

    protected abstract void releaseLastValueArray();

    protected abstract void releaseLastTimeArray();

    public abstract int delete(long lowerBound, long upperBound);

    protected abstract void cloneAs(TVList cloneList);

    public abstract void clear();

    abstract void clearValue();

    protected abstract void clearTime();

    protected abstract void checkExpansion();

    protected abstract Object getPrimitiveArraysByType(TSDataType dataType);

    protected abstract long[] cloneTime(long[] array);

    public abstract TimeValuePair getTimeValuePair(int index);

    protected abstract TimeValuePair getTimeValuePair(
            int index, long time, Integer floatPrecision, TSEncoding encoding);

    protected abstract void writeValidValuesIntoTsBlock(
            TsBlockBuilder builder, int floatPrecision, TSEncoding encoding, List<TimeRange> deletionList);

    public abstract TSDataType getDataType();

    public static TVList deserialize(DataInputStream stream) throws IOException {
        throw new UnsupportedOperationException("Method 'deserialize' is not implemented in the subclass");
    }


}
