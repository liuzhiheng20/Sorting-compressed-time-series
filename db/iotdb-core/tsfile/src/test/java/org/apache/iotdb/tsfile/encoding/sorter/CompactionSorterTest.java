package org.apache.iotdb.tsfile.encoding.sorter;

import org.apache.iotdb.tsfile.encoding.decoder.*;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.utils.CompressedPageData;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;
import org.junit.Test;

import java.io.*;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CompactionSorterTest {
    //10.1.10  100.1.1  1000.10.1 10000.100.1 100000.1000.1
    private static int ROW_NUM = 1000000;
    private static int PAGE_SIZE = 10;
    private static int STEP_SIZE = 1;
    private static int REAPEAT_NUM = 10;
    private static int STARTI_INDEX =0; //700000;60000

    @Test
    public void testCorrect() {
        // 一次测试 依次按照page的size将page按size两两合并
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];
        long[] timeTwoPage = new long[PAGE_SIZE*2];
        long[] valueTwoPage = new long[PAGE_SIZE*2];
        CompressedPageData pageData1;
        CompressedPageData pageData2;
        CompressedPageData pageDataOldMethod1;
        CompressedPageData pageDataOldMethod2;
        prepareData(allTimes, allValues);
        int pageNum = ROW_NUM/PAGE_SIZE;
        int startIndex = 0;
        while(startIndex+2*PAGE_SIZE <= ROW_NUM){
            for(int j=0; j<PAGE_SIZE; j++) {
                timesPage1[j] = allTimes[startIndex+j];
                valuesPage1[j] = allValues[startIndex+j];
                timesPage2[j] = allTimes[startIndex+PAGE_SIZE+j];
                valuesPage2[j] = allValues[startIndex+PAGE_SIZE+j];
            }
            for(int j=0; j<2*PAGE_SIZE; j++) {
                timeTwoPage[j] = allTimes[startIndex+j];
                valueTwoPage[j] = allValues[startIndex+j];
            }
            startIndex += STEP_SIZE;
            sortTimeWithValue(timesPage1, valuesPage1);
            sortTimeWithValue(timesPage2, valuesPage2);
            sortTimeWithValue(timeTwoPage, valueTwoPage);
            if(timesPage2[0] > timesPage1[PAGE_SIZE-1]) {
                continue;  // 如果时间列没有overlap，则直接跳过
            }
            if(timesPage1[0] < timesPage2[0]) {
                pageData1 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageData2 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
                pageDataOldMethod1 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageDataOldMethod2 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
            } else{
                pageData2 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageData1 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
                pageDataOldMethod2 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageDataOldMethod1 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
            }
            LinkedList<CompressedPageData> data = new LinkedList<>();
            data.add(pageData1);
            data.add(pageData2);
            PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
            sorter.sortPage(pageData2);
            CompactionSorterTimeDecoder timeDecoder = new CompactionSorterTimeDecoder(0,0, data);
            CompactionSorterValueDecoder valueDecoder = new CompactionSorterValueDecoder(0, data);
            for(int j=0; j<2*PAGE_SIZE; j++) {
                assertEquals(timeTwoPage[j], timeDecoder.forwardDecode());
                assertEquals(valueTwoPage[j], valueDecoder.forwardDecode());
            }

            //oldCompactionMethod(pageDataOldMethod1, pageDataOldMethod2);
        }
    }

    @Test
    public void testNewCompactionMemory() throws Exception {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];

        long[] timePageTemp;
        long[] valuePageTemp;
        CompressedPageData pageData1;
        CompressedPageData pageData2;
        if(STARTI_INDEX+2*PAGE_SIZE > ROW_NUM) {
            return;
        }
        prepareData(allTimes, allValues);
        for(int j=0; j<PAGE_SIZE; j++) {
            timesPage1[j] = allTimes[STARTI_INDEX+j];
            valuesPage1[j] = allValues[STARTI_INDEX+j];
            timesPage2[j] = allTimes[STARTI_INDEX+PAGE_SIZE+j];
            valuesPage2[j] = allValues[STARTI_INDEX+PAGE_SIZE+j];
        }
        sortTimeWithValue(timesPage1, valuesPage1);
        sortTimeWithValue(timesPage2, valuesPage2);
        if(timesPage2[0] > timesPage1[PAGE_SIZE-1]) {
            return;  // 如果时间列没有overlap，则直接跳过
        }
        if(timesPage1[0] > timesPage2[0]) {
            timePageTemp = timesPage1;
            timesPage1 = timesPage2;
            timesPage2 = timePageTemp;
            valuePageTemp = valuesPage1;
            valuesPage1 = valuesPage2;
            valuesPage2 = valuePageTemp;
        }
        pageData1 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
        pageData2 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);

        allTimes = new long[1];
        allValues = new long[1];
        timesPage1 = new long[1];
        valuesPage1 = new long[1];
        timesPage2 = new long[1];
        valuesPage2 = new long[1];
        timePageTemp = new long[1];
        valuePageTemp = new long[1];
        System.gc();
        synchronized (this) {
            try {
                wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        recordMemory(memoryMXBean);


        LinkedList<CompressedPageData> data = new LinkedList<>();
        data.add(pageData1);
        data.add(pageData2);
        PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
        sorter.sortPage(pageData2);

        recordMemory(memoryMXBean);
    }

    @Test
    public void testOldCompactionMemory() throws Exception {
        MemoryMXBean memoryMXBean = ManagementFactory.getMemoryMXBean();
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];

        long[] timePageTemp;
        long[] valuePageTemp;
        CompressedPageData pageData1;
        CompressedPageData pageData2;
        if(STARTI_INDEX+2*PAGE_SIZE > ROW_NUM) {
            return;
        }
        prepareData(allTimes, allValues);
        for(int j=0; j<PAGE_SIZE; j++) {
            timesPage1[j] = allTimes[STARTI_INDEX+j];
            valuesPage1[j] = allValues[STARTI_INDEX+j];
            timesPage2[j] = allTimes[STARTI_INDEX+PAGE_SIZE+j];
            valuesPage2[j] = allValues[STARTI_INDEX+PAGE_SIZE+j];
        }
        sortTimeWithValue(timesPage1, valuesPage1);
        sortTimeWithValue(timesPage2, valuesPage2);
        if(timesPage2[0] > timesPage1[PAGE_SIZE-1]) {
            return;  // 如果时间列没有overlap，则直接跳过
        }
        if(timesPage1[0] > timesPage2[0]) {
            timePageTemp = timesPage1;
            timesPage1 = timesPage2;
            timesPage2 = timePageTemp;
            valuePageTemp = valuesPage1;
            valuesPage1 = valuesPage2;
            valuesPage2 = valuePageTemp;
        }
        ByteBuffer timeBuffer1TS_2DIFF = getByteBuffer(PAGE_SIZE, timesPage1, false);
        ByteBuffer timeBuffer2TS_2DIFF = getByteBuffer(PAGE_SIZE, timesPage2, false);
        ByteBuffer valueBuffer1TS_2DIFF = getByteBuffer(PAGE_SIZE, valuesPage1, false);
        ByteBuffer valueBuffer2TS_2DIFF = getByteBuffer(PAGE_SIZE, valuesPage2, false);

//        ByteBuffer timeBuffer1Chimp = getByteBuffer(PAGE_SIZE, timesPage1, true);
//        ByteBuffer timeBuffer2Chimp = getByteBuffer(PAGE_SIZE, timesPage2, true);
//        ByteBuffer valueBuffer1Chimp = getByteBuffer(PAGE_SIZE, valuesPage1, true);
//        ByteBuffer valueBuffer2Chimp = getByteBuffer(PAGE_SIZE, valuesPage2, true);


        allTimes = new long[1];
        allValues = new long[1];
        timesPage1 = new long[1];
        valuesPage1 = new long[1];
        timesPage2 = new long[1];
        valuesPage2 = new long[1];
        timePageTemp = new long[1];
        valuePageTemp = new long[1];
        System.gc();
        synchronized (this) {
            try {
                wait(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //recordMemory(memoryMXBean);

        long[] time1 = new long[PAGE_SIZE];
        long[] time2 = new long[PAGE_SIZE];
        long[] value1 = new long[PAGE_SIZE];
        long[] value2 = new long[PAGE_SIZE];
        recordMemory(memoryMXBean);
        CompactionPage(time1, time2, value1, value2, timeBuffer1TS_2DIFF, valueBuffer1TS_2DIFF, timeBuffer2TS_2DIFF, valueBuffer2TS_2DIFF, false);
        //CompactionPage(time1, time2, value1, value2, timeBuffer1Chimp, valueBuffer1Chimp, timeBuffer2Chimp, valueBuffer2Chimp, true);
        recordMemory(memoryMXBean);
        time1[0] = 0;
        time2[0] = 0;
        value1[0] = 0;
        value2[0] = 0;

    }

    @Test
    public void testCompactionTime() throws IOException {
        // 测试compaction方法的时间
        long[] allTimes = new long[ROW_NUM];
        long[] allValues = new long[ROW_NUM];
        long[] timesPage1 = new long[PAGE_SIZE];
        long[] valuesPage1 = new long[PAGE_SIZE];
        long[] timesPage2 = new long[PAGE_SIZE];
        long[] valuesPage2 = new long[PAGE_SIZE];
        long[] timeTwoPage = new long[PAGE_SIZE*2];
        long[] valueTwoPage = new long[PAGE_SIZE*2];

        long[] timePageTemp;
        long[] valuePageTemp;
        CompressedPageData pageData1;
        CompressedPageData pageData2;
        prepareData(allTimes, allValues);
        long totalTimeNew = 0;  // 合并操作的总体用时
        long totalTimeTS_2DIFF = 0;
        long totalTimeChimp = 0;
        long startTime = 0;
        long compactimeNum = 0;   // 执行合并操作的次数
        for(int r=0; r<REAPEAT_NUM; r++){
            int startIndex = 0;
            while(startIndex+2*PAGE_SIZE <= ROW_NUM){
                for(int j=0; j<PAGE_SIZE; j++) {
                    timesPage1[j] = allTimes[startIndex+j];
                    valuesPage1[j] = allValues[startIndex+j];
                    timesPage2[j] = allTimes[startIndex+PAGE_SIZE+j];
                    valuesPage2[j] = allValues[startIndex+PAGE_SIZE+j];
                }
                for(int j=0; j<2*PAGE_SIZE; j++) {
                    timeTwoPage[j] = allTimes[startIndex+j];
                    valueTwoPage[j] = allValues[startIndex+j];
                }
                startIndex += STEP_SIZE;
                sortTimeWithValue(timesPage1, valuesPage1);
                sortTimeWithValue(timesPage2, valuesPage2);
                sortTimeWithValue(timeTwoPage, valueTwoPage);
                if(timesPage2[0] > timesPage1[PAGE_SIZE-1]) {
                    continue;  // 如果时间列没有overlap，则直接跳过
                }
                if(timesPage1[0] > timesPage2[0]) {
                    timePageTemp = timesPage1;
                    timesPage1 = timesPage2;
                    timesPage2 = timePageTemp;
                    valuePageTemp = valuesPage1;
                    valuesPage1 = valuesPage2;
                    valuesPage2 = valuePageTemp;
                }
                pageData1 = getPageData(PAGE_SIZE, timesPage1, valuesPage1);
                pageData2 = getPageData(PAGE_SIZE, timesPage2, valuesPage2);
                ByteBuffer timeBuffer1TS_2DIFF = getByteBuffer(PAGE_SIZE, timesPage1, false);
                ByteBuffer timeBuffer2TS_2DIFF = getByteBuffer(PAGE_SIZE, timesPage2, false);
                ByteBuffer valueBuffer1TS_2DIFF = getByteBuffer(PAGE_SIZE, valuesPage1, false);
                ByteBuffer valueBuffer2TS_2DIFF = getByteBuffer(PAGE_SIZE, valuesPage2, false);
                ByteBuffer timeBuffer1Chimp = getByteBuffer(PAGE_SIZE, timesPage1, true);
                ByteBuffer timeBuffer2Chimp = getByteBuffer(PAGE_SIZE, timesPage2, true);
                ByteBuffer valueBuffer1Chimp = getByteBuffer(PAGE_SIZE, valuesPage1, true);
                ByteBuffer valueBuffer2Chimp = getByteBuffer(PAGE_SIZE, valuesPage2, true);

                LinkedList<CompressedPageData> data = new LinkedList<>();
                data.add(pageData1);
                data.add(pageData2);
                PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
                startTime = System.nanoTime();
                sorter.sortPage(pageData2);
                totalTimeNew += System.nanoTime()-startTime;
                compactimeNum++;

                startTime = System.nanoTime();
                CompactionPage(timeBuffer1TS_2DIFF, valueBuffer1TS_2DIFF, timeBuffer2TS_2DIFF, valueBuffer2TS_2DIFF, false);
                totalTimeTS_2DIFF += System.nanoTime()-startTime;

                startTime = System.nanoTime();
                CompactionPage(timeBuffer1Chimp, valueBuffer1Chimp, timeBuffer2Chimp, valueBuffer2Chimp, true);
                totalTimeChimp += System.nanoTime()-startTime;
            }
        }
        writeDataToTXT(new long[]{compactimeNum, totalTimeNew/compactimeNum, totalTimeTS_2DIFF/compactimeNum, totalTimeChimp/compactimeNum});
    }

    public CompressedPageData getPageData(int rowNum, long[] times, long[] values) {
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<rowNum; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        byte[] temp = new byte[timeEncoder.getValsNum()/4+1];
        System.arraycopy(timeCompressedData.lens, 0, temp, 0, timeEncoder.getValsNum()/4+1);
        timeCompressedData.lens = temp;
        temp = new byte[timeEncoder.getValsNum()/4+1];
        System.arraycopy(valueCompressedData.lens, 0, temp, 0, timeEncoder.getValsNum()/4+1);
        valueCompressedData.lens = temp;
        temp = new byte[timeEncoder.getValsLen()];
        System.arraycopy(timeCompressedData.vals, 0, temp, 0, timeEncoder.getValsLen());
        timeCompressedData.vals = temp;
        temp = new byte[valueEncoder.getValsLen()];
        System.arraycopy(valueCompressedData.vals, 0, temp, 0, valueEncoder.getValsLen());
        valueCompressedData.vals = temp;
        return new CompressedPageData(timeCompressedData, valueCompressedData, PAGE_SIZE, Arrays.stream(times).min().getAsLong(), Arrays.stream(times).max().getAsLong());
    }

    public ByteBuffer getByteBuffer(int rowNum, long[] data, boolean encoderType) throws IOException {
        Encoder timeEncoder;
        if(!encoderType) {
            timeEncoder = new DeltaBinaryEncoder.LongDeltaEncoder();
        } else {
            timeEncoder = new LongChimpEncoder();
        }
        ByteArrayOutputStream dataOutputStream = new ByteArrayOutputStream();
        for(int i=0; i<rowNum; i++) {
            timeEncoder.encode(data[i], dataOutputStream);
        }
        timeEncoder.flush(dataOutputStream);
        ByteBuffer temp = ByteBuffer.wrap(dataOutputStream.toByteArray());
        //dataOutputStream = new ByteArrayOutputStream();
        //System.gc();
        return temp;
    }

    public void CompactionPage(long[] time1, long[] time2,long[] value1, long[] value2, ByteBuffer timeBuffer1, ByteBuffer valueBuffer1, ByteBuffer timeBuffer2, ByteBuffer valueBuffer2, boolean encoderType) throws IOException {
        //encoderType= 0 -> ts_2diff    encoderType= 1 -> Chimp
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
            time1[i] = timeDecoder.readLong(timeBuffer1);
            value1[i] = valueDecoder.readLong(valueBuffer1);
        }
        if(!encoderType) {
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        } else {
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        for(int i=0; i<PAGE_SIZE; i++) {
            time2[i] = timeDecoder.readLong(timeBuffer2);
            value2[i] = valueDecoder.readLong(valueBuffer2);
        }
        int begIndex = 0;
        int endIndex = 0;
        ByteArrayOutputStream timeOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        while(begIndex<PAGE_SIZE || endIndex<PAGE_SIZE) {
            if(begIndex<PAGE_SIZE && endIndex<PAGE_SIZE){
                if(time2[begIndex]<time1[endIndex]) {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
                else {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                }
            } else{
                if(endIndex<PAGE_SIZE) {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                } else {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
            }
        }
        timeEncoder.flush(timeOutputStream);
        valueEncoder.flush(valueOutputStream);
    }

    public void CompactionPage(ByteBuffer timeBuffer1, ByteBuffer valueBuffer1, ByteBuffer timeBuffer2, ByteBuffer valueBuffer2, boolean encoderType) throws IOException {
        //encoderType= 0 -> ts_2diff    encoderType= 1 -> Chimp
        Encoder timeEncoder;
        Encoder valueEncoder;
        Decoder timeDecoder;
        Decoder valueDecoder;
        long[] time1 = new long[PAGE_SIZE];
        long[] time2 = new long[PAGE_SIZE];
        long[] value1 = new long[PAGE_SIZE];
        long[] value2 = new long[PAGE_SIZE];
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
            time1[i] = timeDecoder.readLong(timeBuffer1);
            value1[i] = valueDecoder.readLong(valueBuffer1);
        }
        if(!encoderType) {
            timeDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueDecoder = new DeltaBinaryDecoder.LongDeltaDecoder();
        } else {
            valueDecoder = new LongChimpDecoder();
            timeDecoder = new LongChimpDecoder();
        }
        for(int i=0; i<PAGE_SIZE; i++) {
            time2[i] = timeDecoder.readLong(timeBuffer2);
            value2[i] = valueDecoder.readLong(valueBuffer2);
        }
        int begIndex = 0;
        int endIndex = 0;
        ByteArrayOutputStream timeOutputStream = new ByteArrayOutputStream();
        ByteArrayOutputStream valueOutputStream = new ByteArrayOutputStream();
        while(begIndex<PAGE_SIZE || endIndex<PAGE_SIZE) {
            if(begIndex<PAGE_SIZE && endIndex<PAGE_SIZE){
                if(time2[begIndex]<time1[endIndex]) {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
                else {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                }
            } else{
                if(endIndex<PAGE_SIZE) {
                    timeEncoder.encode(time1[endIndex], timeOutputStream);
                    valueEncoder.encode(value1[endIndex], valueOutputStream);
                    endIndex++;
                } else {
                    timeEncoder.encode(time2[begIndex], timeOutputStream);
                    valueEncoder.encode(value2[begIndex], valueOutputStream);
                    begIndex++;
                }
            }
        }
        timeEncoder.flush(timeOutputStream);
        valueEncoder.flush(valueOutputStream);
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

    public void recordMemory(MemoryMXBean memoryMXBean) {
        MemoryUsage heap = memoryMXBean.getHeapMemoryUsage();
        MemoryUsage nonHeap = memoryMXBean.getNonHeapMemoryUsage();
        writeDataToTXT(new long[]{heap.getUsed(), nonHeap.getUsed()});
    }

    public void prepareData(long[] times, long[] values) {
        //从指定的文件中加载数据集
//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned.csv", 2, ROW_NUM+1, 0, times);
//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", 2, ROW_NUM+1, 1, values);

//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM+1, 0, times);
//        for(int i=0; i<ROW_NUM; i++) {
//            times[i] = 1000*times[i];
//        }
//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM+1, 1, values);

        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201310_cleaned.csv", 2, ROW_NUM, 0, times);

//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 0, times);
//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 1, values);

        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 1, values);

        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 1, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 4, values);

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
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\compaction_time.txt";
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
