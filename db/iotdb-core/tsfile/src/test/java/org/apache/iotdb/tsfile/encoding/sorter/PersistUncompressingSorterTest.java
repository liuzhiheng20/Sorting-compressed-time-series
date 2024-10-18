package org.apache.iotdb.tsfile.encoding.sorter;

import org.apache.iotdb.tsfile.encoding.decoder.CompactionSorterTimeDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.CompactionSorterValueDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSDeltaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.VVarIntEncoder;
import org.apache.iotdb.tsfile.utils.CompressedPageData;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;
import org.junit.Test;

import java.io.*;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class PersistUncompressingSorterTest {
    private static int ROW_NUM = 10;
    private static int REAPEAT_NUM = 10000;
    private static long LOW_BOUND = -1000;
    private static long UP_BOUND = 1000;

    @Test
    public void testSort() throws IOException {
        // 数据缩减合并，测试反向解码的效果
        CompressedPageData pageDataOld = getPageData(new long[] {17001025, 17001030, 17001028, 17001034}, new long[] {991,-640,8,0}, ROW_NUM, LOW_BOUND, UP_BOUND);
        CompressedPageData pageDataNew = getPageData(new long[] {-421,-166,179,}, new long[] {27,11,580},ROW_NUM, LOW_BOUND, UP_BOUND);
        LinkedList<CompressedPageData> data = new LinkedList<>();
        data.add(pageDataOld);
        data.add(pageDataNew);
        PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
        sorter.sortPage(pageDataNew);

        CompactionSorterTimeDecoder timeDecoder = new CompactionSorterTimeDecoder(0,0, data);
        CompactionSorterValueDecoder valueDecoder = new CompactionSorterValueDecoder(0, data);
        long tempTime = 0;
        long tempValue = 0;
        for(int i=0; i<2*ROW_NUM; i++) {
            tempTime = timeDecoder.forwardDecode();
            tempValue = valueDecoder.forwardDecode();
        }
    }

    @Test
    public void testSortRandom() throws IOException {
        for(int j=0; j<REAPEAT_NUM; j++) {
            int[] timesNew = new int[ROW_NUM];
            int[] timesOld = new int[ROW_NUM];
            int[] times = new int[ROW_NUM*2];
            CompressedPageData pageDataOld = getPageData(ROW_NUM, LOW_BOUND, UP_BOUND, timesOld);
            CompressedPageData pageDataNew = getPageData(ROW_NUM, LOW_BOUND, UP_BOUND, timesNew);
            for(int i=0; i<ROW_NUM; i++) {
                times[i] = timesOld[i];
                times[ROW_NUM+i] = timesNew[i];
            }
            LinkedList<CompressedPageData> data = new LinkedList<>();
            if(pageDataOld.getMinTime() < pageDataNew.getMinTime()) {
                data.add(pageDataOld);
                data.add(pageDataNew);
                PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
                sorter.sortPage(pageDataNew);
            } else {
                data.add(pageDataNew);
                data.add(pageDataOld);
                PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
                sorter.sortPage(pageDataOld);
            }
            Arrays.sort(times);
            if(hasDuplicate(times)){
                continue;
            }
            CompactionSorterTimeDecoder timeDecoder = new CompactionSorterTimeDecoder(0,0, data);
            long temp = 0;
            for(int i=0; i<2*ROW_NUM; i++) {
                assertEquals(times[i], timeDecoder.forwardDecode());
            }
        }
    }

    @Test
    public void testSortWithValueRandom() throws IOException {
        for(int j=0; j<REAPEAT_NUM; j++) {
            int[] timesNew = new int[ROW_NUM];
            int[] timesOld = new int[ROW_NUM];
            int[] times = new int[ROW_NUM*2];
            int[] valuesNew = new int[ROW_NUM];
            int[] valuesOld = new int[ROW_NUM];
            int[] values = new int[ROW_NUM*2];
            CompressedPageData pageDataOld = getPageData(ROW_NUM, LOW_BOUND, UP_BOUND, timesOld, valuesOld);
            CompressedPageData pageDataNew = getPageData(ROW_NUM, LOW_BOUND, UP_BOUND, timesNew, valuesNew);
            for(int i=0; i<ROW_NUM; i++) {
                times[i] = timesOld[i];
                times[ROW_NUM+i] = timesNew[i];
                values[i] = valuesOld[i];
                values[ROW_NUM+i] = valuesNew[i];
            }

            long[][] sorted = new long[ROW_NUM*2][2];
            for(int i=0; i<ROW_NUM*2; i++){
                sorted[i][0] = times[i];
                sorted[i][1] = values[i];
            }
            Arrays.sort(sorted, new Comparator<long[]>() {
                @Override
                public int compare(long[] o1, long[] o2) {
                    return (int)(o1[0]-o2[0]);
                }
            });

            LinkedList<CompressedPageData> data = new LinkedList<>();
            if(pageDataOld.getMinTime() < pageDataNew.getMinTime()) {
                data.add(pageDataOld);
                data.add(pageDataNew);
                PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
                sorter.sortPage(pageDataNew);
            } else {
                data.add(pageDataNew);
                data.add(pageDataOld);
                PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
                sorter.sortPage(pageDataOld);
            }
            if(hasDuplicate(times)){
                continue;
            }
            CompactionSorterTimeDecoder timeDecoder = new CompactionSorterTimeDecoder(0,0, data);
            CompactionSorterValueDecoder valueDecoder = new CompactionSorterValueDecoder(0, data);
            for(int i=0; i<2*ROW_NUM; i++) {
                assertEquals(sorted[i][0], timeDecoder.forwardDecode());
                assertEquals(sorted[i][1], valueDecoder.forwardDecode());
            }
        }
    }

    @Test
    public void testSortOriginal() throws IOException {
        //用真实数据集测试压缩合并的效果
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        long[] timesOld = new long[ROW_NUM];
        long[] timesNew = new long[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            timesOld[i] = times.get(i);
            timesNew[i] = times.get(i+ROW_NUM);
        }
        long beginTime = 0;
        long endTime = 0;
        long totalTime = 0;
        for (int j=0; j<REAPEAT_NUM; j++) {
            CompressedPageData pageDataOld = getPageData(timesOld, ROW_NUM, LOW_BOUND, UP_BOUND);
            CompressedPageData pageDataNew = getPageData(timesNew, ROW_NUM, LOW_BOUND, UP_BOUND);
            LinkedList<CompressedPageData> dataNew = new LinkedList<>();
            LinkedList<CompressedPageData> dataOld = new LinkedList<>();
            dataNew.add(pageDataNew);
            dataOld.add(pageDataOld);
            beginTime = System.nanoTime();
            CompactionSorterTimeDecoder timeDecoderOld = new CompactionSorterTimeDecoder(0,0, dataOld);
            CompactionSorterTimeDecoder timeDecoderNew = new CompactionSorterTimeDecoder(0,0, dataNew);
            long[] timeOld = new long[ROW_NUM];
            long[] timeNew = new long[ROW_NUM];
            for(int i=0; i<ROW_NUM; i++) {
                timeOld[i] = timeDecoderOld.forwardDecode();
                timeNew[i] = timeDecoderNew.forwardDecode();
            }
            int begIndex = 0;
            int endIndex = 0;
            TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
            TS_DELTA_data timeCompressedData = new TS_DELTA_data();
            while(begIndex<ROW_NUM || endIndex<ROW_NUM) {
                if(begIndex<ROW_NUM && endIndex<ROW_NUM){
                    if(timeNew[begIndex]<timeOld[endIndex]) {
                        timeEncoder.encode(timeNew[begIndex], timeCompressedData);
                        begIndex++;
                    }
                    else {
                        timeEncoder.encode(timeOld[endIndex], timeCompressedData);
                        endIndex++;
                    }
                } else{
                    if(endIndex<ROW_NUM) {
                        timeEncoder.encode(timeOld[endIndex], timeCompressedData);
                        endIndex++;
                    } else {
                        timeEncoder.encode(timeNew[begIndex], timeCompressedData);
                        begIndex++;
                    }
                }
            }
            endTime = System.nanoTime();
            totalTime += endTime-beginTime;
        }
        writeDataToTXT(new int[]{(int) (totalTime)});
    }


    @Test
    public void testSortRealData() throws IOException {
        //用真实数据集测试压缩合并的效果
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        long[] timesOld = new long[ROW_NUM];
        long[] timesNew = new long[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            timesOld[i] = times.get(i);
            timesNew[i] = times.get(i+ROW_NUM);
        }
        long beginTime = 0;
        long endTime = 0;
        long totalTime = 0;
        for(int j=0; j<REAPEAT_NUM; j++) {
            CompressedPageData pageDataOld = getPageData(timesOld, ROW_NUM, LOW_BOUND, UP_BOUND);
            CompressedPageData pageDataNew = getPageData(timesNew, ROW_NUM, LOW_BOUND, UP_BOUND);
            LinkedList<CompressedPageData> data = new LinkedList<>();
            data.add(pageDataOld);
            data.add(pageDataNew);
            beginTime = System.nanoTime();
            PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
            sorter.sortPage(pageDataNew);
            endTime = System.nanoTime();
            totalTime += endTime-beginTime;
        }
        writeDataToTXT(new int[]{(int) (totalTime)});

        //Collections.sort(times);


//        CompactionSorterTimeDecoder timeDecoder = new CompactionSorterTimeDecoder(0,0, data);
//        long temp = 0;
//        for(int i=0; i<2*ROW_NUM; i++) {
//            assertEquals((long)times.get(i), timeDecoder.forwardDecode());
//        }
    }



    public CompressedPageData getPageData(int rowNum, long lowBound, long upBound, int[] times) {
        int[] values = new int[rowNum];
        for(int i=0; i<rowNum; i++) {
            values[i] = (int) ((Math.random() * (upBound - lowBound + 1)) + lowBound);
            times[i] = (int) ((Math.random() * (upBound - lowBound + 1)) + lowBound);
        }
        Arrays.sort(times);
        writeDataToTXT(times);
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
        return new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
    }

    public CompressedPageData getPageData(int rowNum, long lowBound, long upBound, int[] times, int[] values) {
        for(int i=0; i<rowNum; i++) {
            values[i] = (int) ((Math.random() * (upBound - lowBound + 1)) + lowBound);
            times[i] = (int) ((Math.random() * (upBound - lowBound + 1)) + lowBound);
        }
        Arrays.sort(times);
        writeDataToTXT(times);
        writeDataToTXT(values);
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
        return new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
    }

    public boolean hasDuplicate(int[] array) {
        for (int i = 0; i < array.length; i++) {
            for (int j = i + 1; j < array.length; j++) {
                if (array[i] == array[j]) {
                    return true; // 发现重复元素
                }
            }
        }
        return false;
    }

    public CompressedPageData getPageData(long[] times, int rowNum, long lowBound, long upBound) {
        int[] values = new int[rowNum];
        for(int i=0; i<rowNum; i++) {
            values[i] = (int) ((Math.random() * (upBound - lowBound + 1)) + lowBound);
        }
        Arrays.sort(times);
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
        return new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsLong(), Arrays.stream(times).max().getAsLong());
    }

    public CompressedPageData getPageData(long[] times, long[] values, int rowNum, long lowBound, long upBound) {
        //Arrays.sort(times);
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
        return new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsLong(), Arrays.stream(times).max().getAsLong());
    }

    public void writeDataToTXT(int[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\compaction_data_sort.txt";
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

    public void prepareData(List<Long> times, List<Long> values) {
        //从指定的文件中加载数据集
        //readCSV("D:\\senior\\毕设\\data\\s-10_1e7_div_10_cleaned.csv", 1, 2*ROW_NUM, 0, times);
        readCSV("D:\\senior\\毕设\\data\\201306-citibike-tripdata_digital_1000_cleaned.csv", 1, 2*ROW_NUM, 0, times);
        //readCSV("D:\\senior\\毕设\\data\\201306-citibike-tripdata_digital_1000_cleaned.csv",2, ROW_NUM+1, 3, values);
    }

    public boolean readCSV(String filePath, int line_begin, int line_end, int col, List<Long> times) {
        // 读取csv文件，将第col列从line_begin到line_end行为止的数据读入times中
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            int currentLine = 1;

            while ((line = reader.readLine()) != null) {
                if (currentLine >= line_begin && currentLine <= line_end) {
                    String[] tokens = line.split(",");
                    times.add((long) Double.parseDouble(tokens[col]));
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
}
