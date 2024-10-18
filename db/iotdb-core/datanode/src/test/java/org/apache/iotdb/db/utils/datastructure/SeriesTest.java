package org.apache.iotdb.db.utils.datastructure;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.iotdb.tsfile.encoding.decoder.DeltaDeltaLongDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DoublePrecisionDecoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaDeltaLongEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.RamUsageEstimator;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class SeriesTest {  // 用来测试压缩形式和不压缩形式的时间消耗上的区别
    int ROW_NUM = 100000;
    int REPEAT_NUM = 1;
    private DeltaDeltaLongEncoder timeWriter;
    private DeltaDeltaLongDecoder timeReader;
    private DoublePrecisionEncoderV2 valueWriter;  // gorilla编码
    private DoublePrecisionEncoderV2 valueWriter2;  // gorilla编码
    private DoublePrecisionDecoderV2 valueReader;
    ByteArrayOutputStream timeOut;
    ByteArrayOutputStream valueOut;

    ByteArrayOutputStream valueOut2;
    private ByteBuffer timeBuffer;
    private ByteBuffer valueBuffer;
    @Test
    public void testCorrect() {
        // 测试压缩方法的正确性
    }

    @Test
    public void testInsertTime() {
        // 测试完整的插入时间，包含expand空间的时间
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        final long startTime = System.currentTimeMillis();
        for(int j=0; j<REPEAT_NUM; j++){
            DoubleTVList tvList = new TimDoubleTVList();
            CompressedTVList compressedTVList = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
            for (int i = 0; i < times.size(); i++) {
                //tvList.putDouble(times.get(i), values.get(i));
                compressedTVList.putDouble(times.get(i), values.get(i));
            }
        }
        final long endTime = System.currentTimeMillis();
        addCSV(true, endTime - startTime);
    }

    @Test
    public void testEncodeTime() {
        // 测试单挑序列编码时间
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        DoubleTVList tvList = new TimDoubleTVList();
        for (int i = 0; i < times.size(); i++) {
            tvList.putDouble(times.get(i), values.get(i));
        }
        final long startTime = System.currentTimeMillis();
        times.sort(null);
        for(int j=0; j<REPEAT_NUM; j++){
            timeWriter = new DeltaDeltaLongEncoder();
            valueWriter = new DoublePrecisionEncoderV2();
            timeOut = new ByteArrayOutputStream();
            valueOut = new ByteArrayOutputStream();
            writeTimeData(times);
            writeValueData(values);
        }
        final long endTime = System.currentTimeMillis();
        addCSV(true, endTime - startTime);
    }

    @Test
    public void testDecodeTime() {
        // 测试单挑序列编码时间
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        DoubleTVList tvList = new TimDoubleTVList();
        for (int i = 0; i < times.size(); i++) {
            tvList.putDouble(times.get(i),(double)values.get(i));
        }
        timeWriter = new DeltaDeltaLongEncoder();
        valueWriter = new DoublePrecisionEncoderV2();
        timeOut = new ByteArrayOutputStream();
        valueOut = new ByteArrayOutputStream();
        writeTimeData(times);
        writeValueData(values);
        final long startTime = System.currentTimeMillis();
        for(int j=0; j<REPEAT_NUM; j++){
            timeReader = new DeltaDeltaLongDecoder();
            valueReader = new DoublePrecisionDecoderV2();
            timeReader.reset();
            byte[] timepage = timeOut.toByteArray();
            timeBuffer = ByteBuffer.wrap(timepage);
            byte[] valuepage = valueOut.toByteArray();
            valueBuffer = ByteBuffer.wrap(valuepage);
            while (timeReader.hasNext(timeBuffer)) {
                timeReader.readLong(timeBuffer);
            }
            while (valueReader.hasNext(valueBuffer)) {
                valueReader.readDouble(valueBuffer);
            }
        }
        final long endTime = System.currentTimeMillis();
        addCSV(true, endTime - startTime);
    }

    @Test
    public void testConvertTime() {
        // 测试单条 值序列 利用移动方式加速的效果
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        DoubleTVList tvList = new TimDoubleTVList();
        for (int i = 0; i < times.size(); i++) {
            tvList.putDouble(times.get(i),(double)values.get(i));
        }
        timeWriter = new DeltaDeltaLongEncoder();
        valueWriter = new DoublePrecisionEncoderV2();
        timeOut = new ByteArrayOutputStream();
        valueOut = new ByteArrayOutputStream();
        writeTimeData(times);
        writeValueData(values);
        final long startTime = System.currentTimeMillis();
        for(int j=0; j<REPEAT_NUM; j++){
            valueWriter2 = new DoublePrecisionEncoderV2();
            valueOut2 = new ByteArrayOutputStream();
            valueReader = new DoublePrecisionDecoderV2();
            byte[] valuepage = valueOut.toByteArray();
            valueBuffer = ByteBuffer.wrap(valuepage);
            while (valueReader.hasNext(valueBuffer)) {
//                double res = valueReader.readDouble(valueBuffer);
//                valueWriter2.encode(res, valueOut2);
                long[] res = valueReader.readLong2(valueBuffer);
                if(res[0]<=64){
                    valueWriter2.copyEncode(res, valueOut2);
                } else {
                    valueWriter2.encode(res[2], valueOut2);
                }
            }
        }
        final long endTime = System.currentTimeMillis();
        addCSV(true, endTime - startTime);
    }

    @Test
    public void testEncodeAndSortTime() {
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        final long startTime = System.currentTimeMillis();
        for(int j=0; j<REPEAT_NUM; j++){
            DoubleTVList tvList = new TimDoubleTVList();
            for (int i = 0; i < times.size(); i++) {
                tvList.putDouble(times.get(i),(double)values.get(i));
            }
            //tvList.sort();
        }
        final long endTime = System.currentTimeMillis();
        addCSV(true, endTime - startTime);
    }

    @Test
    public void testConvertAndSortTime() throws IOException {
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);

        final long startTime = System.currentTimeMillis();
        for(int j=0; j<REPEAT_NUM; j++){
            //DoubleTVList tvList = new TimDoubleTVList();
            CompressedTVList compressedTVList = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
            for (int i = 0; i < times.size(); i++) {
                compressedTVList.putDouble(times.get(i),(double)values.get(i));
                //tvList.putDouble(times.get(i),(double)values.get(i));
            }
            //tvList.sort();
            //compressedTVList.convertAndSort2();
            compressedTVList.disordered_uncompressing_sort(1);
            //compressedTVList.convertAndSort_simple();
            //TVList tvlist2 = compressedTVList.convert();
            //long tvlistsize = RamUsageEstimator.sizeOf(tvlist2);
            // tvlistsize++;
//            for(int i=0;i< tvList.rowCount(); i++){
//                assertEquals(tvList.getTime(i), tvlist2.getTime(i));
//                // assertEquals(tvList.getDouble(i), tvlist2.getDouble(i), 0.1);
//                // 上面这里不通过 因为会有相同的时间戳
//            }
        }
        final long endTime = System.currentTimeMillis();
        addCSV(true, endTime - startTime);
    }

    @Test
    public void testSize() {
        // 测试存储大小
        List<Long> times = new ArrayList<>();
        List<Double> values = new ArrayList<>();
        readCSV("D:/senior/DQ/research/data/node1/node1/data_selected.csv",2, 22590, 10, times);
        DoubleTVList tvList = new TimDoubleTVList();
        CompressedTVList compressedTVList = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
        for (int i = 0; i < REPEAT_NUM; i++) {
            //tvList.putDouble(times.get(i), values.get(i));
            compressedTVList.putDouble(times.get(i), values.get(i));
        }
        long tvlistsize = RamUsageEstimator.sizeOf(tvList);
        long compressedtvlistsize = RamUsageEstimator.sizeOf(compressedTVList);
        //TVList t = compressedTVList.convert();
        tvlistsize = 0;
    }

    // 读取csv文件，将第col列从line_begin到line_end行为止的数据读入times中
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
        //readCSV("D:\\senior\\毕设\\data\\s-10_1e7_div_10_cleaned.csv", 1, ROW_NUM, 0, times);
        readCSV("D:\\senior\\毕设\\data\\201306-citibike-tripdata_digital_1000_cleaned.csv", 1, ROW_NUM, 0, times);
        readCSV("D:\\senior\\DQ\\research\\new_encode\\data\\2013-citibike-tripdata\\2013-citibike-tripdata\\201306-citibike-tripdata.csv",2, ROW_NUM+1, 3, values);
    }
    private void writeTimeData(List<Long> times) {
        for (int i = 0; i < times.size(); i++) {
            timeWriter.encode(times.get(i), timeOut);
        }
        timeWriter.flush(timeOut);
    }

    private void writeValueData(List<Long> values) {
        for (int i = 0; i < values.size(); i++) {
            valueWriter.encode((double)values.get(i), valueOut);
        }
        valueWriter.flush(valueOut);
    }

    @Test
    public void testBufferMove() {
        int arraylen = 10;
        byte[] byteArray_1 = new byte[arraylen];
        byte[] byteArray_2 = new byte[arraylen];
        for(int i=0; i<arraylen; i++){
            byteArray_2[i] = -1;
        }
        byteArray_1[0] = -92;
        byteArray_1[1] = -86;
        byteArray_1[2] = 108;
        byteArray_1[3] = -4;
        byteArray_1[4] = -89;
        ByteBuffer buf= ByteBuffer.wrap(byteArray_1);
        buf.position(5);
        CompressedTVList list = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
        list.writeBuffer(byteArray_1, 1, 1, 18, byteArray_2);
        buf.position(9);
    }

    @Test
    public void testBufferOpe() {
        int datalen = 5;
        int arraylen = 10;
        byte[] byteArray_1 = new byte[arraylen];
        byte[] byteArray_2 = new byte[arraylen];
        // 通过随机生成数据检测对于字节流操作的正确性
        // 一个长度为10的byte数组，前5个byte写入数据，后5个byte空白
        Random random = new Random();
        for(int i=0; i<datalen; i++){
            byteArray_1[i] = (byte) random.nextInt(256); // 生成 0 到 255 之间的随机整数，然后转换为 byte 类型
            byteArray_2[i] = byteArray_1[i];
        }

        // 针对数据进行操作
        ByteBuffer buf= ByteBuffer.wrap(byteArray_1);
        buf.position(5);
        int startPos = 1;
        int startOffset = random.nextInt(8);
        int movelen = random.nextInt(40)+1;

        CompressedTVList list = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
        list.rightMoveBuffer(10, byteArray_1, startPos, startOffset, movelen);
        for(int i=0; i<movelen; i++){
            startOffset++;
            if (startOffset==8){
                startPos++;
                startOffset=0;
            }
        }
        buf.position(9);
        list.leftMoveBuffer(byteArray_1, startPos, startOffset, movelen);


        // 检测数据的正确性
        for(int i=0; i<datalen; i++) {
            if(byteArray_1[i]!=byteArray_2[i]){
                byteArray_1[i] = 0;
            }
        }

    }

    @Test
    public void testBufferOpes() {
        for(int i=0; i<100000; i++){
            testBufferOpe();
        }
    }

    @Test
    public void testDelete() throws IOException {
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        final long startTime = System.currentTimeMillis();
            //DoubleTVList tvList = new TimDoubleTVList();
        CompressedTVList compressedTVList = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
        for (int i = 0; i < times.size(); i++) {
            compressedTVList.putDouble(times.get(i),(double)values.get(i));
            // tvList.putDouble(times.get(i),(double)values.get(i));
        }

        compressedTVList.compressedDataDelete(times.get(2), values.get(2));

        TVList tvList = compressedTVList.convert();


        final long endTime = System.currentTimeMillis();
    }

    @Test
    public void testInsert() throws IOException {
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        final long startTime = System.currentTimeMillis();
        //DoubleTVList tvList = new TimDoubleTVList();
        CompressedTVList compressedTVList = new CompressedTVList(TSDataType.getTsDataType((byte) 4));
        for (int i = 0; i < times.size(); i++) {
            compressedTVList.putDouble(times.get(i),(double)values.get(i));
            // tvList.putDouble(times.get(i),(double)values.get(i));
        }

        compressedTVList.compressedDataInsert(times.get(3)+1000, values.get(3));
        TVList tvList = compressedTVList.convert();
        final long endTime = System.currentTimeMillis();
    }


}
