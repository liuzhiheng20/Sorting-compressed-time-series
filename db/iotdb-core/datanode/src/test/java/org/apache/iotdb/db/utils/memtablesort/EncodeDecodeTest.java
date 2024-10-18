package org.apache.iotdb.db.utils.memtablesort;

import org.apache.iotdb.db.utils.compressedsort.TS_DELTA_decoder;
import org.apache.iotdb.db.utils.compressedsort.TS_DELTA_encoder;
import org.apache.iotdb.db.utils.compressedsort.V_VARINT_decoder;
import org.apache.iotdb.db.utils.compressedsort.V_VARINT_encoder;
import org.apache.iotdb.tsfile.encoding.decoder.*;
import org.apache.iotdb.tsfile.encoding.encoder.*;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;

public class EncodeDecodeTest {
    private static int ROW_NUM = 1000000;
    private static int REAPEAT_NUM = 1000;
    private static long LOW_BOUND = -1000;
    private static long UP_BOUND = 1000;
    @Test
    public void testTimeEncoder() throws IOException {
        List<Long> times = new ArrayList<>();
//        for(int i=0; i<ROW_NUM; i++) {
//            data[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
//        }
        prepareData(times);
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(times.get(i), timeCompressedData);
        }
        TS_DELTA_decoder decoder = new TS_DELTA_decoder(0, 0, 0);
        // 检测正向编码
        for(int i=0; i<ROW_NUM; i++){
            assertEquals((long)times.get(i), decoder.forwardDecode(timeCompressedData));
        }
        // 检测反向编码
        for(int i=ROW_NUM-1; i>0; i--) {
            assertEquals((long)times.get(i-1), decoder.backwardDecode(timeCompressedData));
        }
    }

    @Test
    public void testValueEncoder() throws IOException {
        int[] data = new int[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            data[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
        }
        V_VARINT_encoder valueEncoder = new V_VARINT_encoder();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            valueEncoder.encode(data[i], valueCompressedData);
        }
        PublicBAOS valueOut = new PublicBAOS();
        ReadWriteForEncodingUtils.writeUnsignedInt(valueEncoder.getValsNum()+1, valueOut);
        valueOut.write(valueCompressedData.lens, 0, (valueEncoder.getValsNum()-1)/4+1);
        valueOut.write(valueCompressedData.vals, 0, valueEncoder.getValsLen());
        ByteBuffer timeBuffer = ByteBuffer.wrap(valueOut.getBuf());
        V_VARINT_decoder decoder = new V_VARINT_decoder(0, 0, 0);
        // 检测正向编码
        for(int i=0; i<ROW_NUM; i++){
            assertEquals(data[i], decoder.forwardDecode(valueCompressedData));
        }
        // 检测反向编码
        for(int i=ROW_NUM-1; i>=0; i--) {
            assertEquals(data[i], decoder.backwardDecode(valueCompressedData));
        }
    }

    @Test
    public void testTimeEncodeSize() throws IOException {
        TS_DELTA_encoder timeEncoder1 = new TS_DELTA_encoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder timeEncoder3 = new LongChimpEncoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        List<Long> times = new ArrayList<>();
        prepareData(times);
        //Collections.sort(times);
        for(int i=0; i<ROW_NUM-1; i++) {
            timeEncoder1.encode(times.get(i), timeCompressedData);
            timeEncoder2.encode(times.get(i), out2);
            timeEncoder3.encode(times.get(i), out3);
        }
        timeEncoder2.flush(out2);
        timeEncoder3.flush(out3);
        times.get(1);
    }

    @Test
    public void testValueEncodeSize() throws IOException {
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder timeEncoder3 = new LongChimpEncoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        Encoder valueEncoder4 = new LongZigzagEncoder();
        ByteArrayOutputStream out4 = new ByteArrayOutputStream();
        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
        sortTimeWithValue(times, values);
        for(int i=0; i<ROW_NUM-1; i++) {
            valueEncoder.encode(values[i], timeCompressedData);
            timeEncoder2.encode(values[i], out2);
            timeEncoder3.encode(values[i], out3);
            valueEncoder4.encode(values[i], out4);
        }
        timeEncoder2.flush(out2);
        timeEncoder3.flush(out3);
        valueEncoder4.flush(out4);
        times[1] = 0;
    }

    @Test
    public void testEncodeTime() throws IOException {
        long encodeTime1 = 0;
        long encodeTime2 = 0;
        long encodeTime3 = 0;
        long encodeTime4 = 0;
        TS_DELTA_encoder timeEncoder1 = new TS_DELTA_encoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder timeEncoder3 = new LongChimpEncoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        Encoder timeEncoder4 = new PlainEncoder(TSDataType.INT64, 0);
        ByteArrayOutputStream out4 = new ByteArrayOutputStream();
        List<Long> times = new ArrayList<>();
        prepareData(times);
        // 对于citibike数据集，对其中的元素放大1000倍，单位变成毫秒
//        for (int i = 0; i < times.size(); i++) {
//            times.set(i, times.get(i) * 1000);
//        }
        Collections.sort(times);
        for(int j=0; j<REAPEAT_NUM; j++) {
            // 累计一次编码1的时间
            timeEncoder1 = new TS_DELTA_encoder();
            timeCompressedData = new TS_DELTA_data();
            long startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder1.encode(times.get(i), timeCompressedData);
            }
            encodeTime1 += System.currentTimeMillis()-startTime;

            // 累计一次编码1的时间
            timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
            out2 = new ByteArrayOutputStream();
            startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder2.encode(times.get(i), out2);
            }
            encodeTime2 += System.currentTimeMillis()-startTime;

            // 累计一次编码1的时间
            timeEncoder3 = new LongChimpEncoder();
            out3 = new ByteArrayOutputStream();
            startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder3.encode(times.get(i), out3);
            }
            encodeTime3 += System.currentTimeMillis()-startTime;

            // 累计一次编码1的时间
            timeEncoder4 = new PlainEncoder(TSDataType.INT64, 0);
            out4 = new ByteArrayOutputStream();
            startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder4.encode(times.get(i), out4);
            }
            encodeTime4 += System.currentTimeMillis()-startTime;

        }
        // 将结果写入文件
        writeDataToTXT(new long[] {encodeTime1, encodeTime2, encodeTime3, encodeTime4});
    }


    @Test
    public void testDecodeTime() throws IOException {
        long decodeTime1 = 0;
        long backwardDecodeTime1 = 0;
        long decodeTime2 = 0;
        long decodeTime3 = 0;
        long decodeTime4 = 0;
        TS_DELTA_encoder timeEncoder1 = new TS_DELTA_encoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        TS_DELTA_decoder timeDecoder1;
        TS_DELTA_decoder timeBackwardDecoder1;
        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        Decoder timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder timeEncoder3 = new LongChimpEncoder();
        Decoder timeDecoder3 = new LongChimpDecoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        Encoder timeEncoder4 = new PlainEncoder(TSDataType.INT64, 0);
        Decoder timeDecoder4 = new PlainDecoder();
        ByteArrayOutputStream out4 = new ByteArrayOutputStream();
        List<Long> times = new ArrayList<>();
        prepareData(times);
        // 对于citibike数据集，对其中的元素放大1000倍，单位变成毫秒
//        for (int i = 0; i < times.size(); i++) {
//            times.set(i, times.get(i) * 1000);
//        }
        //Collections.sort(times);
        for(int j=0; j<REAPEAT_NUM; j++) {
            // 累计一次解码1的时间
            timeEncoder1 = new TS_DELTA_encoder();
            timeCompressedData = new TS_DELTA_data();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder1.encode(times.get(i), timeCompressedData);
            }
            //测试正向解码forward decode
            timeDecoder1 = new TS_DELTA_decoder(true, 0, 0, 0);
//            long startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder1.forwardDecode(timeCompressedData);
            }
//            decodeTime1 += System.currentTimeMillis()-startTime;
            //测试反向解码backward decode
            timeBackwardDecoder1 = new TS_DELTA_decoder(timeDecoder1.getNowValue(), timeDecoder1.getNowNum(), timeDecoder1.getNowPos());
            long startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeBackwardDecoder1.backwardDecode(timeCompressedData);
            }
            backwardDecodeTime1 += System.currentTimeMillis()-startTime;

            // 累计一次解码2的时间
            timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
            out2 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder2.encode(times.get(i), out2);
            }
            timeEncoder2.flush(out2);
            ByteBuffer buffer2 = ByteBuffer.wrap(out2.toByteArray());
            timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
            startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder2.readLong(buffer2);
            }
            decodeTime2 += System.currentTimeMillis()-startTime;

            // 累计一次解码3的时间
            timeEncoder3 = new LongChimpEncoder();
            out3 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder3.encode(times.get(i), out3);
            }
            timeEncoder3.flush(out3);
            ByteBuffer buffer3 = ByteBuffer.wrap(out3.toByteArray());
            timeDecoder3 = new LongChimpDecoder();
            startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder3.readLong(buffer3);
            }
            decodeTime3 += System.currentTimeMillis()-startTime;

            // 累计一次解码4的时间
            timeEncoder4 = new PlainEncoder(TSDataType.INT64, 0);
            out4 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder4.encode(times.get(i), out4);
            }
            timeEncoder4.flush(out4);
            ByteBuffer buffer4 = ByteBuffer.wrap(out4.toByteArray());
            timeDecoder4 = new PlainDecoder();
            startTime = System.currentTimeMillis();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder4.readLong(buffer4);
            }
            decodeTime4 += System.currentTimeMillis()-startTime;

        }
        // 将结果写入文件
        writeDataToTXT(new long[] {decodeTime1, decodeTime2, decodeTime3, decodeTime4});
    }


    @Test
    public void testTimeValueEncodeDecodeTime() throws IOException {
        // total是指时间列和空间列一起考虑，但是编码和解码时间仍然是分开统计
        long encodeTime1 = 0;
        long encodeTime2 = 0;
        long encodeTime3 = 0;

        long decodeTime1 = 0;
        long decodeTime2 = 0;
        long decodeTime3 = 0;
        TS_DELTA_encoder timeEncoder1 = new TS_DELTA_encoder();
        VVarIntEncoder valueEncoder1 = new VVarIntEncoder();
        TS_DELTA_data timeCompressedData1 = new TS_DELTA_data();
        TS_DELTA_data valueCompressedData1 = new TS_DELTA_data();
        TS_DELTA_decoder timeDecoder1 = new TS_DELTA_decoder(0, 0, 0);
        V_VARINT_decoder valueDecoder1 = new V_VARINT_decoder(true,0, 0, 0);

        Encoder timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        Decoder timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        Encoder valueEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
        Decoder valueDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
        ByteArrayOutputStream valueOut2 = new ByteArrayOutputStream();

        Encoder timeEncoder3 = new LongChimpEncoder();
        Decoder timeDecoder3 = new LongChimpDecoder();
        ByteArrayOutputStream out3 = new ByteArrayOutputStream();
        Encoder valueEncoder3 = new LongChimpEncoder();
        Decoder valueDecoder3 = new LongChimpDecoder();
        ByteArrayOutputStream valueOut3 = new ByteArrayOutputStream();


        long[] times = new long[ROW_NUM];
        long[] values = new long[ROW_NUM];
        prepareData(times, values);
         //对于citibike数据集，对其中的元素放大1000倍，单位变成毫秒
        sortTimeWithValue(times, values);
        for(int i=1; i<ROW_NUM;i++){
            if(times[i]<times[i-1]){
                return;
            }
        }
        for(int j=0; j<REAPEAT_NUM; j++) {
            // 累计一次ts_delta的时间
            long startTime = System.currentTimeMillis();
            timeEncoder1 = new TS_DELTA_encoder();
            valueEncoder1 = new VVarIntEncoder();
            timeCompressedData1 = new TS_DELTA_data();
            valueCompressedData1 = new TS_DELTA_data();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder1.encode(times[i], timeCompressedData1);
                valueEncoder1.encode(values[i], valueCompressedData1);
            }
            encodeTime1 += System.currentTimeMillis()-startTime;
            //测试正向解码forward decode
            startTime = System.currentTimeMillis();
            timeDecoder1 = new TS_DELTA_decoder(0, 0, 0);
            valueDecoder1 = new V_VARINT_decoder(true,0, 0, 0);
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder1.forwardDecode(timeCompressedData1);
                valueDecoder1.forwardDecode(valueCompressedData1);
                //assert (valueDecoder1.forwardDecode(valueCompressedData1);
            }
            decodeTime1 += System.currentTimeMillis()-startTime;

            // 累计一次ts_2diff的时间
            startTime = System.currentTimeMillis();
            timeEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
            out2 = new ByteArrayOutputStream();
            valueEncoder2 = new DeltaBinaryEncoder.LongDeltaEncoder();
            valueOut2 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder2.encode(times[i], out2);
                valueEncoder2.encode(values[i], valueOut2);
            }
            timeEncoder2.flush(out2);
            valueEncoder2.flush(valueOut2);
            encodeTime2 += System.currentTimeMillis()-startTime;
            startTime = System.currentTimeMillis();
            ByteBuffer timeBuffer2 = ByteBuffer.wrap(out2.toByteArray());
            ByteBuffer valueBuffer2 = ByteBuffer.wrap(valueOut2.toByteArray());
            timeDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
            valueDecoder2 = new DeltaBinaryDecoder.LongDeltaDecoder();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder2.readLong(timeBuffer2);
                valueDecoder2.readLong(valueBuffer2);
            }
            decodeTime2 += System.currentTimeMillis()-startTime;

            // 累计一次chimp的时间
            startTime = System.currentTimeMillis();
            timeEncoder3 = new LongChimpEncoder();
            out3 = new ByteArrayOutputStream();
            valueEncoder3 = new LongChimpEncoder();
            valueOut3 = new ByteArrayOutputStream();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeEncoder3.encode(times[i], out3);
                valueEncoder3.encode(values[i], valueOut3);
            }
            timeEncoder3.flush(out3);
            valueEncoder3.flush(valueOut3);
            encodeTime3 += System.currentTimeMillis()-startTime;
            startTime = System.currentTimeMillis();
            ByteBuffer buffer3 = ByteBuffer.wrap(out3.toByteArray());
            ByteBuffer valueBuffer3 = ByteBuffer.wrap(valueOut3.toByteArray());
            timeDecoder3 = new LongChimpDecoder();
            valueDecoder3 = new LongChimpDecoder();
            for(int i=0; i<ROW_NUM-1; i++) {
                timeDecoder3.readLong(buffer3);
                valueDecoder3.readLong(valueBuffer3);
            }
            decodeTime3 += System.currentTimeMillis()-startTime;
        }
        // 将结果写入文件
        writeDataToTXT(new long[] {encodeTime1, encodeTime2, encodeTime3, decodeTime1, decodeTime2, decodeTime3});
    }

    public void prepareData(List<Long> times) {
        //从指定的文件中加载数据集
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned.csv", 2, ROW_NUM, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201310_cleaned.csv", 2, ROW_NUM, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/SteeringAngle_TypeA(DOUBLE).csv", 2, ROW_NUM+1, 0, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 1, times);
    }

    public void prepareData(long[] times, long[] values) {
        //从指定的文件中加载数据集
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned.csv", 2, ROW_NUM+1, 1, values);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/samsung/s-10_cleaned_new.csv", 2, ROW_NUM+1, 1, values);

//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM+1, 0, times);
//        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201306_cleaned.csv", 2, ROW_NUM+1, 1, values);

        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/swzl/swzl_clean.csv", 2, ROW_NUM+1, 1, values);

        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 1, times);
        readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/ship/shipNet.csv", 2, ROW_NUM+1, 4, values);

        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/real/citibike/citibike201310_cleaned.csv", 2, ROW_NUM, 0, times);

        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 0, times);
        //readCSV("D:/senior/DQ/research/compressed_sort_paper/dataset/artificial/exponential/exponential_1_1000.csv", 2, ROW_NUM+1, 1, values);
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

    public void writeDataToTXT(long[] data) {
        String filePath = "D:\\senior\\DQ\\research\\compressed_sort\\test\\encode_decode_time.txt";
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
