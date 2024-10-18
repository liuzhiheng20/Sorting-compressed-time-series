package org.apache.iotdb.db.utils.datastructure;

import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class CompressedTVListTest {

    @Test
    public void test() {
        // 一些基本情况的test
        int[][] irregularArray1 = new int[][] {
                {1, 2},
                {3, 4, 5},
                {6, 7, 8, 9}
        };
        int[][] irregularArray2 = new int[][] {
                {1, 2, 3, 4},
                {3, 4, 5, 6},
                {6, 7, 8, 9}
        };

        long size1 = RamUsageEstimator.sizeOf(irregularArray1);
        long size2 = RamUsageEstimator.sizeOf(irregularArray2);
        size2 = 0;
    }

    @Test
    public void testSize() {
        // 测试存储大小
        List<Long> times = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        readCSV("D:/senior/DQ/research/data/node1/node1/data_selected.csv",2, 22590, 10, times, values);
        DoubleTVList tvList = new TimDoubleTVList();
        CompressedTVList compressedTVList = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
        for (int i = 0; i < 1; i++) {
            tvList.putDouble(times.get(i), values.get(i));
            compressedTVList.putDouble(times.get(i), values.get(i));
        }
        long tvlistsize = RamUsageEstimator.sizeOf(tvList);
        long compressedtvlistsize = RamUsageEstimator.sizeOf(compressedTVList);
        //TVList t = compressedTVList.convert();
        tvlistsize = 0;
    }



    public boolean readCSV(String filePath, int line_begin, int line_end, int col, List<Long> times, List<Double> values) {
        //String filePath = "D:/senior/DQ/research/data/node1/node1/data1/root.toyotads-3/_146629-119204515-0.csv";
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            int currentLine = 1;

            while ((line = reader.readLine()) != null) {
                if (currentLine >= line_begin && currentLine <= line_end) {
                    String[] tokens = line.split(",");
                    times.add((long) Double.parseDouble(tokens[0]));
                    values.add(Double.parseDouble(tokens[col]));
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
        String filePath = "D:/senior/DQ/research/survey/timeUnion/result.csv";
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
}
