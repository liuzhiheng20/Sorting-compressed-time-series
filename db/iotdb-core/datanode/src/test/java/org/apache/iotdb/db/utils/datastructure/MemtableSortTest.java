package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.utils.compressedsort.CompressedDataSorter;
import org.apache.iotdb.db.utils.compressedsort.TS_DELTA_decoder;
import org.apache.iotdb.db.utils.compressedsort.V_VARINT_decoder;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaBinaryDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.LongChimpDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.PlainDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;
import org.junit.Test;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class MemtableSortTest {
    //10.10.10, 10.10.100,10.100.1000,10.1000.10000
    private static int ROW_NUM = 1000000;
    private static int REAPEAT_NUM = 100;
    private static int STEP_SIZE = 10000;
    private static int PAGE_SIZE = 1000000;
    @Test
    public void testSortCorrect() throws IOException {
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        OptionalLong maxOptional = Arrays.stream(times).max();
        long max = maxOptional.orElseThrow(() -> new RuntimeException("数组为空，无法找出最大值"));
        times[ROW_NUM - 1] = max+3000;
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        //sort
        CompressedDataSorter sorter = new CompressedDataSorter(timeCompressedData, valueCompressedData);
        sorter.blockSort(0,ROW_NUM-1, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());

        V_VARINT_decoder valueDecoder = new V_VARINT_decoder(0, 0, 0);
        TS_DELTA_decoder timeDecoder = new TS_DELTA_decoder(0, 0, 0);

        sortTimeWithValue(times, values);
        // 检测正向编码
        for(int i=0; i<ROW_NUM; i++){
            assertEquals(times[i], timeDecoder.forwardDecode(timeCompressedData));
            assertEquals(values[i], valueDecoder.forwardDecode(valueCompressedData));
        }
    }

    @Test
    public void testNewSortMemory() throws InterruptedException {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        OptionalLong maxOptional = Arrays.stream(times).max();
        long max = maxOptional.orElseThrow(() -> new RuntimeException("数组为空，无法找出最大值"));
        times[ROW_NUM - 1] = max+3000;
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        times = new long[1];
        values = new long[1];
        int timeLen = timeEncoder.getValsLen();
        int valueLen = valueEncoder.getValsLen();
        timeCompressedData.reset(ROW_NUM, timeLen);
        valueCompressedData.reset(ROW_NUM, valueLen);
        System.gc();
        synchronized (this) {
            try {
                wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        recordMemory(memoryMXBean);
        CompressedDataSorter sorter = new CompressedDataSorter(timeCompressedData, valueCompressedData);
        sorter.blockSort(0,ROW_NUM-1, 8, timeLen, 8, valueLen);
        recordMemory(memoryMXBean);
    }

    @Test
    public void testMemory() throws IOException {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        Encoder valueEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
//        Encoder timeEncoder2 = new LongChimpEncoder();
//        Encoder valueEncoder2 = new LongChimpEncoder();
        ByteArrayOutputStream timeStream2 = new ByteArrayOutputStream();
        ByteArrayOutputStream valueStream2 = new ByteArrayOutputStream();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder2.encode(times[i], timeStream2);
            valueEncoder2.encode(values[i], valueStream2);
        }
        times = new long[1];
        values = new long[1];
        timeEncoder2.flush(timeStream2);
        valueEncoder2.flush(valueStream2);
        ByteBuffer timeBuffer2 = ByteBuffer.wrap(timeStream2.toByteArray());
        ByteBuffer valueBuffer2 = ByteBuffer.wrap(valueStream2.toByteArray());
        System.gc();
        synchronized (this) {
            try {
                wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        recordMemory(memoryMXBean);
        SortMemory(timeBuffer2, valueBuffer2, false, true);
        recordMemory(memoryMXBean);
    }

    public void SortMemory(ByteBuffer timeBuffer, ByteBuffer valueBuffer, boolean encoderType, boolean tvListType) {
        //tvListType = 0 -> timTVList   tvListType = 1 -> backTVList
        //encoderType= 0 -> ts_2diff    encoderType= 1 -> Chimp
        TVList tvList;
        if(!tvListType) {
            tvList = new TimLongTVList();
        } else {
            tvList = new BackLongTVList();
        }
        Encoder timeEncoder;
        Encoder valueEncoder;
        Decoder timeDecoder;
        Decoder valueDecoder;
        if(!encoderType) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        } else {
            timeEncoder = new LongChimpEncoder();
            valueEncoder = new LongChimpEncoder();
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        for(int i=0; i<PAGE_SIZE; i++) {
            tvList.putLong(timeDecoder.readLong(timeBuffer), valueDecoder.readLong(valueBuffer));
        }
        tvList.sort();

        ByteArrayOutputStream timeOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        for(int i=0; i<PAGE_SIZE; i++) {
            timeEncoder.encode(tvList.getTime(i), timeOutputStream);
            valueEncoder.encode(tvList.getLong(i), valueOutputStream);
        }
    }

    @Test
    public void testSortTime() throws IOException {
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        long newTime = 0;
        long TS_2DIFF_TimTime = 0;
        long Chimp_TimTime = 0;
        long TS_2DIFF_BackTime= 0;
        long Chimp_BackTime = 0;
        long startTime= 0;
        long sortNum = 0;
        prepareData(times, values);
        for(int r=0; r<REAPEAT_NUM; r++) {
            int startIndex = 0;
            while(startIndex+PAGE_SIZE <= ROW_NUM) {
                TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
                VVarIntEncoder valueEncoder = new VVarIntEncoder();
                TS_DELTA_data timeCompressedData = new TS_DELTA_data();
                TS_DELTA_data valueCompressedData = new TS_DELTA_data();

                Encoder TS_2DIFF_TimtimeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                Encoder TS_2DIFF_TimvalueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                ByteArrayOutputStream TS_2DIFF_TimtimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream TS_2DIFF_TimvalueStream = new ByteArrayOutputStream();

                Encoder Chimp_TimtimeEncoder = new LongChimpEncoder();
                Encoder Chimp_TimvalueEncoder = new LongChimpEncoder();
                ByteArrayOutputStream Chimp_TimtimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream Chimp_TimvalueStream = new ByteArrayOutputStream();

                Encoder TS_2DIFF_BacktimeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                Encoder TS_2DIFF_BackvalueEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
                ByteArrayOutputStream TS_2DIFF_BacktimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream TS_2DIFF_BackvalueStream = new ByteArrayOutputStream();

                Encoder Chimp_BacktimeEncoder = new LongChimpEncoder();
                Encoder Chimp_BackvalueEncoder = new LongChimpEncoder();
                ByteArrayOutputStream Chimp_BacktimeStream = new ByteArrayOutputStream();
                ByteArrayOutputStream Chimp_BackvalueStream = new ByteArrayOutputStream();
                long maxTime = 0;
                for (int i = 0; i < PAGE_SIZE; i++) {
                    if(times[startIndex+i]>maxTime) {
                        maxTime = times[startIndex+i];
                    }
                    timeEncoder.encode(times[startIndex+i], timeCompressedData);
                    valueEncoder.encode(values[startIndex+i], valueCompressedData);
                    TS_2DIFF_TimtimeEncoder.encode(times[startIndex+i], TS_2DIFF_TimtimeStream);
                    TS_2DIFF_TimvalueEncoder.encode(values[startIndex+i], TS_2DIFF_TimvalueStream);
                    Chimp_TimtimeEncoder.encode(times[startIndex+i], Chimp_TimtimeStream);
                    Chimp_TimvalueEncoder.encode(values[startIndex+i], Chimp_TimvalueStream);
                    TS_2DIFF_BacktimeEncoder.encode(times[startIndex+i], TS_2DIFF_BacktimeStream);
                    TS_2DIFF_BackvalueEncoder.encode(values[startIndex+i], TS_2DIFF_BackvalueStream);
                    Chimp_BacktimeEncoder.encode(times[startIndex+i], Chimp_BacktimeStream);
                    Chimp_BackvalueEncoder.encode(values[startIndex+i], Chimp_BackvalueStream);
                }
                timeEncoder.encode(maxTime, timeCompressedData);
                valueEncoder.encode(values[startIndex], valueCompressedData);
                TS_2DIFF_TimtimeEncoder.flush(TS_2DIFF_TimtimeStream);
                TS_2DIFF_TimvalueEncoder.flush(TS_2DIFF_TimvalueStream);
                Chimp_TimtimeEncoder.flush(Chimp_TimtimeStream);
                Chimp_TimvalueEncoder.flush(Chimp_TimvalueStream);
                TS_2DIFF_BacktimeEncoder.flush(TS_2DIFF_BacktimeStream);
                TS_2DIFF_BackvalueEncoder.flush(TS_2DIFF_BackvalueStream);
                Chimp_BacktimeEncoder.flush(Chimp_BacktimeStream);
                Chimp_BackvalueEncoder.flush(Chimp_BackvalueStream);

                startTime = System.nanoTime();
                CompressedDataSorter sorter = new CompressedDataSorter(timeCompressedData, valueCompressedData);
                sorter.blockSort(0, PAGE_SIZE, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());
                newTime += System.nanoTime() - startTime;

                ByteBuffer timeBuffer = ByteBuffer.wrap(TS_2DIFF_TimtimeStream.toByteArray());
                ByteBuffer valueBuffer = ByteBuffer.wrap(TS_2DIFF_TimvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer, valueBuffer, false, false);
                TS_2DIFF_TimTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer2 = ByteBuffer.wrap(Chimp_TimtimeStream.toByteArray());
                ByteBuffer valueBuffer2 = ByteBuffer.wrap(Chimp_TimvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer2, valueBuffer2, true, false);
                Chimp_TimTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer3 = ByteBuffer.wrap(TS_2DIFF_BacktimeStream.toByteArray());
                ByteBuffer valueBuffer3 = ByteBuffer.wrap(TS_2DIFF_BackvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer3, valueBuffer3, false, true);
                TS_2DIFF_BackTime += System.nanoTime()-startTime;

                ByteBuffer timeBuffer4 = ByteBuffer.wrap(Chimp_BacktimeStream.toByteArray());
                ByteBuffer valueBuffer4 = ByteBuffer.wrap(Chimp_BackvalueStream.toByteArray());
                startTime = System.nanoTime();
                SortMemory(timeBuffer4, valueBuffer4, true, true);
                Chimp_BackTime += System.nanoTime()-startTime;


                startIndex += STEP_SIZE;
                sortNum++;
            }
        }
        writeDataToTXT(new long[] {sortNum, newTime/sortNum, TS_2DIFF_TimTime/sortNum, Chimp_TimTime/sortNum, TS_2DIFF_BackTime/sortNum, Chimp_BackTime/sortNum});
    }

    @Test
    public void testEncodeDecodeTime() throws IOException {
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        long[] pageTime = new long[PAGE_SIZE];
        long[] pageValues = new long[PAGE_SIZE];
        long totalTime_TS_DELTA = 0;
        long totalTime_TS_2DIFF = 0;
        long totalTime_CHIMP = 0;
        long sortNum = 0;
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        Decoder timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder timeEncoder3 = new LongChimpEncoder();
        Decoder timeDecoder3 = new LongChimpDecoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        prepareData(times, values);
        for(int r=0; r<REAPEAT_NUM; r++) {
            int startIndex = 0;
            while(startIndex+PAGE_SIZE <= ROW_NUM) {
                for(int i=0; i<PAGE_SIZE; i++){
                    pageTime[i] = times[startIndex+i];
                    pageValues[i] = values[startIndex+i];
                }
                totalTime_TS_DELTA += encodeDecode(pageTime, pageValues);
                timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
                timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
                out2 = new ByteArrayOutputStream();
                totalTime_TS_2DIFF += encodeDecode(pageTime, pageValues, timeEncoder2, timeDecoder2, out2);
                timeEncoder3 = new LongChimpEncoder();
                timeDecoder3 = new LongChimpDecoder();
                out3 = new ByteArrayOutputStream();
                totalTime_CHIMP += encodeDecode(pageTime, pageValues, timeEncoder3, timeDecoder3, out3);
                startIndex += STEP_SIZE;
                sortNum++;
            }
        }
        writeDataToTXT(new long[] {sortNum, totalTime_TS_DELTA/sortNum, totalTime_TS_2DIFF/sortNum, totalTime_CHIMP/sortNum});
    }

    public long encodeDecode(long[] times, long[] values) {
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        long encodeDecodeTime = 0;
        long startTime = System.nanoTime();
        for (int i = 0; i < PAGE_SIZE; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        encodeDecodeTime += System.nanoTime()-startTime;
        V_VARINT_decoder valueDecoder = new V_VARINT_decoder(0, 0, 0);
        TS_DELTA_decoder timeDecoder = new TS_DELTA_decoder(0, 0, 0);
        startTime = System.nanoTime();
        for (int i = 0; i < PAGE_SIZE; i++) {
            timeDecoder.forwardDecode(timeCompressedData);
            valueDecoder.forwardDecode(valueCompressedData);
        }
        encodeDecodeTime += System.nanoTime()-startTime;
        return encodeDecodeTime;
    }

    public long encodeDecode(long[] times, long[] values, Encoder timeEncoder, Decoder timeDecoder, ByteArrayOutputStream out) throws IOException {
        long encodeDecodeTime = 0;
        long startTime = System.nanoTime();
        for (int i = 0; i < PAGE_SIZE; i++) {
            timeEncoder.encode(times[i], out);
        }
        timeEncoder.flush(out);
        encodeDecodeTime += System.nanoTime()-startTime;
        ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());
        startTime = System.nanoTime();
        for (int i = 0; i < PAGE_SIZE; i++) {
            timeDecoder.readLong(buffer);
        }
        encodeDecodeTime += System.nanoTime()-startTime;
        return encodeDecodeTime;
    }

    public void prepareData(long[] times, long[] values) {
        //从指定的文件中加载数据集
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", 2, ROW_NUM+1, 1, values);

//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM+1, 0, times);
//        for(int i=0; i<ROW_NUM; i++) {
//            times[i] = 1000*times[i];
//        }
//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM+1, 1, values);

        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201310_cleaned.csv", 2, ROW_NUM, 0, times);

//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 0, times);
//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 1, values);

        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 0, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 1, values);

        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 1, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 4, values);

    }

    public void sortTimeWithValue(long[] times, long[] values) {
        // 将数据列和时间列根据时间列的大小进行排序
        long[][] sorted = new long[times.length][2];
        for(int i=0; i< times.length; i++){
            sorted[i][0] = times[i];
            sorted[i][1] = values[i];
        }
        Arrays.sort(sorted, (a, b) -> Long.compare(a[0], b[0]));
        for(int i=0; i<times.length; i++) {
            times[i] = sorted[i][0];
            values[i] = sorted[i][1];
        }
    }

    public boolean readCSV(String filePath, int line_begin, int line_end, int col, long[] times) {
        // 读取csv文件，将第col列从line_begin到line_end行为止的数据读入times中
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            int currentLine = 1;
            int index = 0;

            while ((line = reader.readLine()) != null) {
                if (currentLine >= line_begin && currentLine <= line_end) {
                    String[] tokens = line.split(",");
                    times[index] = (long) Double.parseDouble(tokens[col]);
                    index++;
                }
                currentLine++;
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void writeDataToTXT(long[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\memtable_sort_memory.txt";
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

    public void recordMemory(MemoryMXBean memoryMXBean) {
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();
        writeDataToTXT(new long[]{heap.getUsed(), nonHeap.getUsed(), nonHeap.getCommitted(), nonHeap.getMax()});
    }
}
