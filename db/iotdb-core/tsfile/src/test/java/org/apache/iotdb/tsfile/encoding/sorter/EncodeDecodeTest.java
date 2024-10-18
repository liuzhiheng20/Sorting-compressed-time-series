package org.apache.iotdb.tsfile.encoding.sorter;

import org.apache.iotdb.tsfile.encoding.decoder.CompactionSorterTimeDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.CompactionSorterValueDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSDeltaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.VVarIntEncoder;
import org.apache.iotdb.tsfile.utils.CompressedPageData;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.LinkedList;

import static org.junit.Assert.assertEquals;

public class EncodeDecodeTest {
    private static int ROW_NUM = 20;
    private static int REAPEAT_NUM = 10000;
    private static long LOW_BOUND = -10000;
    private static long UP_BOUND = 10000;
    @Test
    public void testCorrect1() throws IOException {
        // 数组没有缩减，测试最简单的正向解码的情况
        int[] times = new int[ROW_NUM];
        int[] values = new int[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            values[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
            times[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
        }
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times[i], timeCompressedData);
            valueEncoder.encode(values[i], valueCompressedData);
        }
        CompressedPageData pageData = new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
        LinkedList<CompressedPageData> data = new LinkedList<>();
        data.add(pageData);
        CompactionSorterTimeDecoder timeDecoder = new CompactionSorterTimeDecoder(0,0, data);
        CompactionSorterValueDecoder valueDecoder = new CompactionSorterValueDecoder(0, data);

        // 检测正向编码
        for(int i=0; i<ROW_NUM; i++){
            assertEquals(times[i], timeDecoder.forwardDecode());
        }
        // 检测反向编码
        for(int i=0; i<ROW_NUM; i++) {
            assertEquals(values[i], valueDecoder.forwardDecode());
        }
    }

    @Test
    public void testCorrect2() throws IOException {
        // 数据缩减合并，测试反向解码的效果
        int[] times = new int[ROW_NUM];
        int[] values = new int[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            values[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
            times[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
        }
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
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

        CompressedPageData pageData = new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
        LinkedList<CompressedPageData> data = new LinkedList<>();
        data.add(pageData);

        TS_DELTA_data timeCompressedData2 = new TS_DELTA_data();
        TS_DELTA_data valueCompressedData2 = new TS_DELTA_data();
        CompressedPageData pageData2 = new CompressedPageData(timeCompressedData2, valueCompressedData2, 0, 0, 0);
        data.add(pageData2);

        CompactionSorterTimeDecoder timeDecoder = new CompactionSorterTimeDecoder(1,times[ROW_NUM-1], data);
        CompactionSorterValueDecoder valueDecoder = new CompactionSorterValueDecoder(1, data);

        // 检测正向编码
        for(int i=ROW_NUM-2; i>0; i--){
            assertEquals(times[i], timeDecoder.backwardDecode());
        }
        // 检测反向编码
        for(int i=ROW_NUM-1; i>0; i--) {
            assertEquals(values[i], valueDecoder.backwardDecode());
        }
    }

    @Test
    public void testDataMove() {
        for(int j=0; j<REAPEAT_NUM;j++) {
            int[] times = new int[ROW_NUM];
            //int[] times = new int[] {-7691, 5735};
            int[] values = new int[ROW_NUM];
            for (int i = 0; i < ROW_NUM; i++) {
                values[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
                times[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
            }
            // writeDataToTXT(times);
            TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
            TS_DELTA_data timeCompressedData = new TS_DELTA_data();
            VVarIntEncoder valueEncoder = new VVarIntEncoder();
            TS_DELTA_data valueCompressedData = new TS_DELTA_data();
            for (int i = 0; i < ROW_NUM; i++) {
                timeEncoder.encode(times[i], timeCompressedData);
                valueEncoder.encode(values[i], valueCompressedData);
            }
            byte[] temp = new byte[timeEncoder.getValsNum() / 4 + 1];
            System.arraycopy(timeCompressedData.lens, 0, temp, 0, timeEncoder.getValsNum() / 4 + 1);
            timeCompressedData.lens = temp;
            temp = new byte[timeEncoder.getValsNum() / 4 + 1];
            System.arraycopy(valueCompressedData.lens, 0, temp, 0, timeEncoder.getValsNum() / 4 + 1);
            valueCompressedData.lens = temp;
            temp = new byte[timeEncoder.getValsLen()];
            System.arraycopy(timeCompressedData.vals, 0, temp, 0, timeEncoder.getValsLen());
            timeCompressedData.vals = temp;
            temp = new byte[valueEncoder.getValsLen()];
            System.arraycopy(valueCompressedData.vals, 0, temp, 0, valueEncoder.getValsLen());
            valueCompressedData.vals = temp;

            CompressedPageData pageData1 = new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
            LinkedList<CompressedPageData> data = new LinkedList<>();
            data.add(pageData1);
            TS_DELTA_data timeCompressedData2 = new TS_DELTA_data();
            TS_DELTA_data valueCompressedData2 = new TS_DELTA_data();
            temp = new byte[timeEncoder.getValsNum() / 4 + 1];
            System.arraycopy(timeCompressedData.lens, 0, temp, 0, timeEncoder.getValsNum() / 4 + 1);
            timeCompressedData2.lens = temp;
            temp = new byte[timeEncoder.getValsNum() / 4 + 1];
            System.arraycopy(valueCompressedData.lens, 0, temp, 0, timeEncoder.getValsNum() / 4 + 1);
            valueCompressedData2.lens = temp;
            temp = new byte[timeEncoder.getValsLen()];
            System.arraycopy(timeCompressedData.vals, 0, temp, 0, timeEncoder.getValsLen());
            timeCompressedData2.vals = temp;
            temp = new byte[valueEncoder.getValsLen()];
            System.arraycopy(valueCompressedData.vals, 0, temp, 0, valueEncoder.getValsLen());
            valueCompressedData2.vals = temp;

            CompressedPageData pageData2 = new CompressedPageData(timeCompressedData2, valueCompressedData2, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
            data.add(pageData2);

            PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
            byte[] dataToMove = new byte[pageData1.getPageTimeLen()+ pageData2.getPageTimeLen()];
            System.arraycopy(pageData1.getTimeData().vals, 0, dataToMove, 0, pageData1.getTimeData().vals.length);
            System.arraycopy(pageData2.getTimeData().vals, 0, dataToMove, pageData1.getTimeData().vals.length, pageData2.getTimeData().vals.length);
            int[] randomInts = randomInts(0, pageData1.getPageTimeLen()+ pageData2.getPageTimeLen(), 3);
            //int[] randomInts = new int[] {12, 16, 20};
            // writeDataToTXT(randomInts);
            sorter.dataMove(dataToMove, randomInts[0], randomInts[1], randomInts[2]);
            sorter.dataMove(true, randomInts[0]/ pageData1.getPageTimeLen(), randomInts[0]% pageData1.getPageTimeLen(), randomInts[1]/ pageData1.getPageTimeLen(), randomInts[1]% pageData1.getPageTimeLen(), randomInts[2]/ pageData1.getPageTimeLen(), randomInts[2]% pageData1.getPageTimeLen());

            for (int i = 0; i < pageData1.getPageTimeLen(); i++) {
                assertEquals(dataToMove[i], pageData1.getTimeData().vals[i]);
            }
            for (int i = 0; i < pageData2.getPageTimeLen(); i++) {
                assertEquals(dataToMove[i+pageData1.getPageTimeLen()], pageData2.getTimeData().vals[i]);
            }

        }
    }

    @Test
    public void testLenDataMove() {
        for(int j=0; j<REAPEAT_NUM;j++) {
            int[] times = new int[ROW_NUM];
            //int[] times = new int[] {8437,-7009,2068,-1658,-6995,-5793,8492,6214};
            int[] values = new int[ROW_NUM];
            for (int i = 0; i < ROW_NUM; i++) {
                values[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
                times[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
            }
            writeDataToTXT(times);
            TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
            TS_DELTA_data timeCompressedData = new TS_DELTA_data();
            VVarIntEncoder valueEncoder = new VVarIntEncoder();
            TS_DELTA_data valueCompressedData = new TS_DELTA_data();
            for (int i = 0; i < ROW_NUM; i++) {
                timeEncoder.encode(times[i], timeCompressedData);
                valueEncoder.encode(values[i], valueCompressedData);
            }
            byte[] temp = new byte[(timeEncoder.getValsNum()+3) / 4];
            System.arraycopy(timeCompressedData.lens, 0, temp, 0, (timeEncoder.getValsNum()+3) / 4);
            timeCompressedData.lens = temp;
            temp = new byte[(timeEncoder.getValsNum()+3) / 4];
            System.arraycopy(valueCompressedData.lens, 0, temp, 0, (timeEncoder.getValsNum()+3) / 4 );
            valueCompressedData.lens = temp;
            temp = new byte[timeEncoder.getValsLen()];
            System.arraycopy(timeCompressedData.vals, 0, temp, 0, timeEncoder.getValsLen());
            timeCompressedData.vals = temp;
            temp = new byte[valueEncoder.getValsLen()];
            System.arraycopy(valueCompressedData.vals, 0, temp, 0, valueEncoder.getValsLen());
            valueCompressedData.vals = temp;

            CompressedPageData pageData1 = new CompressedPageData(timeCompressedData, valueCompressedData, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
            LinkedList<CompressedPageData> data = new LinkedList<>();
            data.add(pageData1);
            TS_DELTA_data timeCompressedData2 = new TS_DELTA_data();
            TS_DELTA_data valueCompressedData2 = new TS_DELTA_data();
            temp = new byte[(timeEncoder.getValsNum()+3) / 4];
            System.arraycopy(timeCompressedData.lens, 0, temp, 0, (timeEncoder.getValsNum()+3) / 4);
            timeCompressedData2.lens = temp;
            temp = new byte[(timeEncoder.getValsNum()+3) / 4];
            System.arraycopy(valueCompressedData.lens, 0, temp, 0, (timeEncoder.getValsNum()+3) / 4);
            valueCompressedData2.lens = temp;
            temp = new byte[timeEncoder.getValsLen()];
            System.arraycopy(timeCompressedData.vals, 0, temp, 0, timeEncoder.getValsLen());
            timeCompressedData2.vals = temp;
            temp = new byte[valueEncoder.getValsLen()];
            System.arraycopy(valueCompressedData.vals, 0, temp, 0, valueEncoder.getValsLen());
            valueCompressedData2.vals = temp;

            CompressedPageData pageData2 = new CompressedPageData(timeCompressedData2, valueCompressedData2, ROW_NUM, Arrays.stream(times).min().getAsInt(), Arrays.stream(times).max().getAsInt());
            data.add(pageData2);

            PersistUncompressingSorter sorter = new PersistUncompressingSorter(data);
            byte[] dataToMove = new byte[pageData1.getTimeData().lens.length+ pageData2.getTimeData().lens.length];
            System.arraycopy(pageData1.getTimeData().lens, 0, dataToMove, 0, pageData1.getTimeData().lens.length);
            System.arraycopy(pageData2.getTimeData().lens, 0, dataToMove, pageData1.getTimeData().lens.length, pageData2.getTimeData().lens.length);
            int[] randomInts = randomInts(0, pageData1.getCount()+ pageData2.getCount(), 3);
            //int[] randomInts = new int[] {0, 5, 11};
            writeDataToTXT(randomInts);
            sorter.lenDataMove(dataToMove, randomInts[0], randomInts[1], randomInts[2]);
            sorter.lenDataMove(true, randomInts[0]/ pageData1.getCount(), randomInts[0]% pageData1.getCount(), randomInts[1]/ pageData1.getCount(), randomInts[1]% pageData1.getCount(), randomInts[2]/ pageData1.getCount(), randomInts[2]% pageData1.getCount());

            for (int i = 0; i < pageData1.getTimeData().lens.length; i++) {
                assertEquals(dataToMove[i], pageData1.getTimeData().lens[i]);
            }
            for (int i = 0; i < pageData2.getTimeData().lens.length; i++) {
                assertEquals(dataToMove[i+pageData1.getTimeData().lens.length], pageData2.getTimeData().lens[i]);
            }
        }
    }

    public int[] randomInts(int lowBound, int upBound, int size) {
        int[] randomInts = new int[size];
        for(int i=0; i<size; i++) {
            randomInts[i] = (int) (Math.random() * ((upBound - lowBound) + 1) + lowBound);
        }
        Arrays.sort(randomInts);
        return randomInts;
    }

    @Test
    public void testCorrect3() throws IOException {
        // 数据缩减合并同时有一些偏移，测试反向，正向解码的效果
    }

    public void writeDataToTXT(int[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\compaction_data_move.txt";
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
