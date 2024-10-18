/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.wal.buffer.IWALByteBufferView;
import org.apache.iotdb.db.utils.MemUtils;
import org.apache.iotdb.db.utils.datastructure.CompressedLongTVList;
import org.apache.iotdb.db.utils.datastructure.CompressedTVList;
import org.apache.iotdb.db.utils.datastructure.Series;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.List;

public class WritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  //private TVList list;
  private Series list;;  //change!
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(WritableMemChunk.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  public WritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    //this.list = TVList.newList(schema.getType());
    //change!
    if(CONFIG.isMemtableCompressed()) {
      this.list = new CompressedTVList(schema.getType());
    } else{
      this.list = TVList.newList(schema.getType());
    }
  }


  public WritableMemChunk() {}

  private void convert() {  //change!将compressed类型的series转成普通类型的tvlist
    if (this.list instanceof CompressedTVList){
        Series temp = null;
        try {
            temp = this.list.convert();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        this.list = temp;
    }
  }

  private void CompressedSort() {  //change!将compressed类型的series转成普通类型的tvlist
    if (this.list instanceof CompressedTVList){
        try {
            ((CompressedTVList) this.list).convertAndSort2();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //writeDataToTXT(new long[]{1});
  }
  public void writeDataToTXT(long[] data) {
    String filePath = "D:\\senior\\毕设\\画图\\实验章节\\内存压缩与不压缩flush\\list大小.txt";
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



  @Override
  public boolean writeWithFlushCheck(long insertTime, Object objectValue) {
    switch (schema.getType()) {
      case BOOLEAN:
        putBoolean(insertTime, (boolean) objectValue);
        break;
      case INT32:
        putInt(insertTime, (int) objectValue);
        break;
      case INT64:
        putLong(insertTime, (long) objectValue);
        break;
      case FLOAT:
        putFloat(insertTime, (float) objectValue);
        break;
      case DOUBLE:
        putDouble(insertTime, (double) objectValue);
        break;
      case TEXT:
        return putBinaryWithFlushCheck(insertTime, (Binary) objectValue);
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
    }
    return false;
  }

  @Override
  public boolean writeAlignedValueWithFlushCheck(
      long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public boolean writeWithFlushCheck(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    switch (dataType) {
      case BOOLEAN:
        boolean[] boolValues = (boolean[]) valueList;
        putBooleans(times, boolValues, bitMap, start, end);
        break;
      case INT32:
        int[] intValues = (int[]) valueList;
        putInts(times, intValues, bitMap, start, end);
        break;
      case INT64:
        long[] longValues = (long[]) valueList;
        putLongs(times, longValues, bitMap, start, end);
        break;
      case FLOAT:
        float[] floatValues = (float[]) valueList;
        putFloats(times, floatValues, bitMap, start, end);
        break;
      case DOUBLE:
        double[] doubleValues = (double[]) valueList;
        putDoubles(times, doubleValues, bitMap, start, end);
        break;
      case TEXT:
        Binary[] binaryValues = (Binary[]) valueList;
        return putBinariesWithFlushCheck(times, binaryValues, bitMap, start, end);
      default:
        throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + dataType);
    }
    return false;
  }

  @Override
  public boolean writeAlignedValuesWithFlushCheck(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  @Override
  public void putLong(long t, long v) {
    list.putLong(t, v);
  }

  @Override
  public void putInt(long t, int v) {
    list.putInt(t, v);
  }

  @Override
  public void putFloat(long t, float v) {
    list.putFloat(t, v);
  }

  @Override
  public void putDouble(long t, double v) {
    list.putDouble(t, v);
  }

  @Override
  public boolean putBinaryWithFlushCheck(long t, Binary v) {
    list.putBinary(t, v);
    return list.reachMaxChunkSizeThreshold();
  }

  @Override
  public void putBoolean(long t, boolean v) {
    list.putBoolean(t, v);
  }

  @Override
  public boolean putAlignedValueWithFlushCheck(long t, Object[] v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {
    list.putLongs(t, v, bitMap, start, end);
  }

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {
    list.putInts(t, v, bitMap, start, end);
  }

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {
    list.putFloats(t, v, bitMap, start, end);
  }

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {
    list.putDoubles(t, v, bitMap, start, end);
  }

  @Override
  public boolean putBinariesWithFlushCheck(
      long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    list.putBinaries(t, v, bitMap, start, end);
    return list.reachMaxChunkSizeThreshold();
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    list.putBooleans(t, v, bitMap, start, end);
  }

  @Override
  public boolean putAlignedValuesWithFlushCheck(
      long[] t, Object[] v, BitMap[] bitMaps, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public synchronized TVList getSortedTvListForQuery() {
    convert(); //change!
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return (TVList) list;
  }

  @Override
  public synchronized TVList getSortedTvListForQuery(List<IMeasurementSchema> measurementSchema) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + list.getDataType());
  }

  private void sortTVList() {
    // convert(); //change!
    // check reference count
    // System.out.println(list instanceof CompressedTVList);
    // writeDataToTXT(new long[]{RamUsageEstimator.sizeOf(list)});
    if ((list.getReferenceCount() > 0 && !list.isSorted())) {
      convert(); //change!
      list = list.clone();  // 因为referencecount>0
    }


    if (!list.isSorted()) {
      //System.out.println("sort one list");  //change!
      if(list instanceof CompressedTVList){
        //System.out.println("convert one list");  //change!
        CompressedSort(); //change!
      } else{
        list.sort();
      }

    }
  }

  @Override
  public synchronized void sortTvListForFlush() {
    sortTVList();
  }

  @Override
  public TVList getTVList() {
    return (TVList)list;
  }

  public CompressedTVList getTVList2() {
    return (CompressedTVList) list;
  }

  public Series getSeries() {
    return list;
  }

  @Override
  public long count() {
    //convert();  //change!
    return list.rowCount();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public long getMaxTime() {
    convert();  //change!
    return list.getMaxTime();
  }

  @Override
  public long getFirstPoint() {
    convert();  //change!
    if (list.rowCount() == 0) {
      return Long.MAX_VALUE;
    }
    return getSortedTvListForQuery().getTimeValuePair(0).getTimestamp();
  }

  @Override
  public long getLastPoint() {
    convert();  //change!
    if (list.rowCount() == 0) {
      return Long.MIN_VALUE;
    }
    return getSortedTvListForQuery()
        .getTimeValuePair(getSortedTvListForQuery().rowCount() - 1)
        .getTimestamp();
  }

  @Override
  public boolean isEmpty() {
    convert();  //change!
    return list.rowCount() == 0;
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    convert();  //change!
    return list.delete(lowerBound, upperBound);
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new ChunkWriterImpl(schema);
  }

  @Override
  public String toString() {
    convert();  //change!
    int size = list.rowCount();
    int firstIndex = 0;
    int lastIndex = size - 1;
    long minTime = Long.MAX_VALUE;
    long maxTime = Long.MIN_VALUE;
    for (int i = 0; i < size; i++) {
      long currentTime = list.getTime(i);
      if (currentTime < minTime) {
        firstIndex = i;
        minTime = currentTime;
      }
      if (currentTime >= maxTime) {
        lastIndex = i;
        maxTime = currentTime;
      }
    }

    StringBuilder out = new StringBuilder("MemChunk Size: " + size + System.lineSeparator());
    if (size != 0) {
      out.append("Data type:").append(schema.getType()).append(System.lineSeparator());
      out.append("First point:")
          .append(list.getTimeValuePair(firstIndex))
          .append(System.lineSeparator());
      out.append("Last point:")
          .append(list.getTimeValuePair(lastIndex))
          .append(System.lineSeparator());
    }
    return out.toString();
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {
    ChunkWriterImpl chunkWriterImpl = (ChunkWriterImpl) chunkWriter;
    //change!之前是一个一个data编码，现在如果数据在写入的过程中已经编码，那么写入时就可以直接写入
    if(list instanceof CompressedTVList || list instanceof CompressedLongTVList){  // 如果数据在内存中已经按照压缩方式存储
      chunkWriterImpl.write(list.getTimeOut(), list.getValueOut(), list.getStatistics());
      chunkWriterImpl.setLastPoint(true);
      return;
    }

    for (int sortedRowIndex = 0; sortedRowIndex < list.rowCount(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);

      TSDataType tsDataType = schema.getType();

      // skip duplicated data
      if ((sortedRowIndex + 1 < list.rowCount() && (time == list.getTime(sortedRowIndex + 1)))) {
        long recordSize =
            MemUtils.getRecordSize(
                tsDataType,
                tsDataType == TSDataType.TEXT ? list.getBinary(sortedRowIndex) : null,
                true);
        CompressionRatio.decreaseDuplicatedMemorySize(recordSize);
        continue;
      }

      // store last point for SDT
      if (sortedRowIndex + 1 == list.rowCount()) {
        chunkWriterImpl.setLastPoint(true);
      }

      switch (tsDataType) {
        case BOOLEAN:
          chunkWriterImpl.write(time, list.getBoolean(sortedRowIndex));
          break;
        case INT32:
          chunkWriterImpl.write(time, list.getInt(sortedRowIndex));
          break;
        case INT64:
          chunkWriterImpl.write(time, list.getLong(sortedRowIndex));
          break;
        case FLOAT:
          chunkWriterImpl.write(time, list.getFloat(sortedRowIndex));
          break;
        case DOUBLE:
          chunkWriterImpl.write(time, list.getDouble(sortedRowIndex));
          break;
        case TEXT:
          chunkWriterImpl.write(time, list.getBinary(sortedRowIndex));
          break;
        default:
          LOGGER.error("WritableMemChunk does not support data type: {}", tsDataType);
          break;
      }
    }
  }

  @Override
  public void release() {
    if (list.getReferenceCount() == 0) {
      list.clear();
    }
  }

  @Override
  public int serializedSize() {
    return schema.serializedSize() + list.serializedSize();
  }

  @Override
  public void serializeToWAL(IWALByteBufferView buffer) {
    byte[] bytes = new byte[schema.serializedSize()];
    schema.serializeTo(ByteBuffer.wrap(bytes));
    buffer.put(bytes);

    list.serializeToWAL(buffer);
  }

  public static WritableMemChunk deserialize(DataInputStream stream) throws IOException {
    WritableMemChunk memChunk = new WritableMemChunk();
    memChunk.schema = MeasurementSchema.deserializeFrom(stream);
    memChunk.list = TVList.deserialize(stream);
    return memChunk;
  }
}
