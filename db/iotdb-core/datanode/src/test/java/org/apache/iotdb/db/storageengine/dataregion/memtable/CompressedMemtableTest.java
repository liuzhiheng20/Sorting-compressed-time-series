package org.apache.iotdb.db.storageengine.dataregion.memtable;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.storageengine.dataregion.flush.MemTableFlushTask;
import org.apache.iotdb.db.utils.constant.TestConstant;
import org.apache.iotdb.db.utils.datastructure.CompressedTVList;
import org.apache.iotdb.db.utils.datastructure.DoubleTVList;
import org.apache.iotdb.db.utils.datastructure.TimDoubleTVList;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;
import org.junit.Test;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class CompressedMemtableTest {
    private static String filePath =
            TestConstant.OUTPUT_DATA_DIR.concat("testUnsealedTsFileProcessor.tsfile");
    private static String storageGroup = "storage_group1";

    private static RestorableTsFileIOWriter writer;
    private static String database = "root.test";
    private static String dataRegionId = "1";
    private static String deviceId = "d0";
    private static TSDataType tsDataType = TSDataType.DOUBLE;

    private long ROW_NUM = 10000000;
    private long memtableSizeThreshold = 100000;
    private long recordUnit = 100000;
    private long times[];
    private double values[];
    private double indexs[];

    @Test
    public void testMemtableSize() throws IllegalPathException {
        // 从csv中读取数据写入memtable
        // 测试在内存压缩和不压缩的情况下，不同的memtable大小能够存储多少的数据
        prepareData();

        IMemTable memTable = new PrimitiveMemTable(database, dataRegionId);
        for (int i = 0; i < ROW_NUM; i++) {
            memTable.write(
                    DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
                    Collections.singletonList(
                            new MeasurementSchema(String.valueOf(indexs[i]), tsDataType, TSEncoding.PLAIN)),
                    times[i],
                    new Object[] {values[i]});
            if(i%recordUnit == 0){
                //writeDataToTXT(new long[]{i, RamUsageEstimator.sizeOf(memTable)});
            }
        }
        //writeDataToTXT(new long[]{ROW_NUM, RamUsageEstimator.sizeOf(memTable)});

    }

    @Test
    public void testWriteAndFlushTime() throws IOException, ExecutionException, InterruptedException, IllegalPathException {
        // 测试存储大小
        prepareData();
        final long startTime = System.currentTimeMillis();
        IMemTable memTable = new PrimitiveMemTable(database, dataRegionId);
        for (int i = 0; i < ROW_NUM; i++) {
            memTable.write(
                    DeviceIDFactory.getInstance().getDeviceID(new PartialPath(deviceId)),
                    Collections.singletonList(
                            new MeasurementSchema(String.valueOf(indexs[i]), tsDataType, TSEncoding.PLAIN)),
                    times[i],
                    new Object[] {values[i]});
            if(i%recordUnit == 0){
                //writeDataToTXT(new long[]{i, RamUsageEstimator.sizeOf(memTable)});
            }
        }
        //writeDataToTXT(new long[]{ROW_NUM, RamUsageEstimator.sizeOf(memTable)});

        writer = new RestorableTsFileIOWriter(FSFactoryProducer.getFSFactory().getFile(filePath));
        MemTableFlushTask memTableFlushTask =
                new MemTableFlushTask(memTable, writer, storageGroup, dataRegionId);
        memTableFlushTask.syncFlushMemTable();

        final long endTime = System.currentTimeMillis();
        writeDataToTXT(new long[]{endTime - startTime});
    }

    public void prepareData() {
        //从共享单车数据集中加载数据
        times = new long[(int) ROW_NUM];
        values = new double[(int) ROW_NUM];
        indexs = new double[(int) ROW_NUM];
        //readCSV("D:/senior/毕设/画图/实验章节/内存压缩与不压缩的memtable/data/citibike_total.csv", times,  indexs, values);
        readCSV("D:\\senior\\DQ\\research\\data\\node1\\node1\\zl_total.csv", times, values, indexs);
    }

    public boolean readCSV(String filePath, long[] times, double[] values, double[] indexs) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(filePath));
            String line;
            int row_index = 0;
            while ((line = reader.readLine()) != null) {
                String[] tokens = line.split(",");
                times[row_index] = (long)Double.parseDouble(tokens[0]);
                values[row_index] = Double.parseDouble(tokens[1]);
                indexs[row_index] = Double.parseDouble(tokens[2]);
                row_index++;
                if(row_index%recordUnit == 0){
                    //writeDataToTXT(new long[]{row_index});
                    if(row_index == 100*recordUnit){
                        break;
                    }
                }
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
        String filePath = "D:\\senior\\毕设\\画图\\实验章节\\内存压缩与不压缩flush\\time.txt";
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
