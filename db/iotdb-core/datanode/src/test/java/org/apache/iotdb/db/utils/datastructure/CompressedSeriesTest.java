package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.utils.compressedsort.CompressedDataSorter;
import org.apache.iotdb.db.utils.compressedsort.TS_DELTA_decoder;
import org.apache.iotdb.db.utils.compressedsort.V_VARINT_decoder;
import org.apache.iotdb.tsfile.encoding.decoder.VarDeltaLongDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSDeltaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.VVarIntEncoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class CompressedSeriesTest {

    private static int ROW_NUM = 100;
    private static int REAPEAT_NUM = 10000;
    private static long LOW_BOUND = -100000;
    private static long UP_BOUND = 100000;
    private static long TIME_LOW_BOUND = 10;

    @Test
    public void testSortCorrectFixedValue() throws IOException {
        int[] time = new int[ROW_NUM];
        int[] value = new int[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            value[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
        }
        time[0] = 1;
        time[1] = 2419;
        time[2] = 412;
        time[3] = 6663;
        time[4] = 2287;
        writeDataToTXT(time);
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(time[i], timeCompressedData);
            valueEncoder.encode(value[i], valueCompressedData);
        }
        //sort
        CompressedDataSorter sorter = new CompressedDataSorter(timeCompressedData, valueCompressedData);
        sorter.blockSort(0,ROW_NUM-1, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());
        Arrays.sort(time);
        //exam
        TS_DELTA_decoder timeDecoder = new TS_DELTA_decoder(0, 0, 0);
        // 检测正向编码
        for(int i=0; i<ROW_NUM; i++){
            assertEquals(time[i], timeDecoder.forwardDecode(timeCompressedData));
        }
    }

    @Test
    public void testSortCorrectRandom() throws IOException {
        for(int j=0; j<REAPEAT_NUM; j++){
            int[] time = new int[ROW_NUM];
            int[] value = new int[ROW_NUM];
            for(int i=0; i<ROW_NUM; i++) {
                time[i] = (int) ((Math.random() * (UP_BOUND - TIME_LOW_BOUND + 1)) + TIME_LOW_BOUND);
                value[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
            }
            time[0] = 1;
            writeDataToTXT(time);
            TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
            VVarIntEncoder valueEncoder = new VVarIntEncoder();
            TS_DELTA_data timeCompressedData = new TS_DELTA_data();
            TS_DELTA_data valueCompressedData = new TS_DELTA_data();
            for (int i=0; i<ROW_NUM; i++) {
                timeEncoder.encode(time[i], timeCompressedData);
                valueEncoder.encode(value[i], valueCompressedData);
            }
            //sort
            CompressedDataSorter sorter = new CompressedDataSorter(timeCompressedData, valueCompressedData);
            sorter.blockSort(0,ROW_NUM-1, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());
            long[][] sorted = new long[ROW_NUM][2];
            for(int i=0; i<ROW_NUM; i++){
                sorted[i][0] = time[i];
                sorted[i][1] = value[i];
            }
            Arrays.sort(sorted, new Comparator<long[]>() {
                @Override
                public int compare(long[] o1, long[] o2) {
                    return (int)(o1[0]-o2[0]);
                }
            });
            //exam
            TS_DELTA_decoder timeDecoder = new TS_DELTA_decoder(0, 0, 0);
            V_VARINT_decoder valueDecoder = new V_VARINT_decoder(true,0, 0, 0);
            // 检测正向编码
            for(int i=0; i<ROW_NUM; i++){
                assertEquals(sorted[i][0], timeDecoder.forwardDecode(timeCompressedData));
                assertEquals(sorted[i][1], valueDecoder.forwardDecode(valueCompressedData));
            }
        }
    }

    @Test
    public void testSortCorrectFile() throws IOException {
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);

        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times.get(i), timeCompressedData);
            valueEncoder.encode(values.get(i), valueCompressedData);
        }
        //sort
        CompressedDataSorter sorter = new CompressedDataSorter(timeCompressedData, valueCompressedData);
        sorter.blockSort(0,ROW_NUM-1, 8, timeEncoder.getValsLen(), 8, valueEncoder.getValsLen());
        Collections.sort(times);
        //exam
        TS_DELTA_decoder timeDecoder = new TS_DELTA_decoder(0, 0, 0);
        // 检测正向编码
        for(int i=0; i<ROW_NUM; i++){
            assertEquals((long)times.get(i), timeDecoder.forwardDecode(timeCompressedData));
        }
    }

    @Test
    public void testConvertAndSortTime() throws IOException {
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);

        final long startTime = System.currentTimeMillis();
        for(int j=0; j<REAPEAT_NUM; j++){
            CompressedLongTVList compressedTVList = new CompressedLongTVList(TSDataType.getTsDataType((byte) 2));  // 值域类型为long的tvlist
            for (int i = 0; i < times.size(); i++) {
                compressedTVList.putLong(times.get(i),values.get(i));
            }
            //tvList.sort();
            compressedTVList.persistent_compressing_sort();
//            for(int i=0;i< tvList.rowCount(); i++){
//                assertEquals(tvList.getTime(i), tvlist2.getTime(i));
//                // assertEquals(tvList.getDouble(i), tvlist2.getDouble(i), 0.1);
//                // 上面这里不通过 因为会有相同的时间戳
//            }
        }
        final long endTime = System.currentTimeMillis();
        addCSV(true, endTime - startTime);
    }

    public boolean readCSV(String filePath, int line_begin, int line_end, int col, List<Long> times) {
        //String filePath = "D:/senior/DQ/research/data/node1/node1/data1/root.toyotads-3/_146629-119204515-0.csv";
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

    public boolean readCSVTime(String filePath, int line_begin, int line_end, int col, List<Long> times) {
        //String filePath = "D:/senior/DQ/research/data/node1/node1/data1/root.toyotads-3/_146629-119204515-0.csv";
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            int currentLine = 1;
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            while ((line = reader.readLine()) != null) {
                if (currentLine >= line_begin && currentLine <= line_end) {
                    String[] tokens = line.split(",");

                    try {
                        String datastr = tokens[col].substring(1, tokens[col].length()-1);;
                        Date date = dateFormat.parse(datastr);
                        long timeInMillis = date.getTime();
                        //System.out.println("Long value: " + timeInMillis);
                        times.add(timeInMillis);
                    } catch (Exception e) {
                        //System.out.println("ParseException: " + e.getMessage());
                    }

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

    public boolean addCSV(boolean lineFeed, long size) {
        String filePath = "D:\\senior\\毕设\\画图\\实验章节\\不同排序算法的耗时情况\\separate_sort.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            String content = Long.toString(size);
            if (lineFeed) {
                writer.write(content);
                writer.newLine();
            } else {
                writer.write(content + ",");
            }
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public void prepareData(List<Long> times, List<Long> values) {
        //从指定的文件中加载数据集
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201310_cleaned.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 0, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 1, values);

    }

    public void  writeDataToTXT(int[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\memtable_sort.txt";
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
