package org.apache.iotdb.tsfile.encoding.decoder.delta;

import org.apache.iotdb.tsfile.encoding.decoder.DeltaDeltaLongDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaGorillaDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.DeltaGorillaSorter;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaDeltaLongEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaGorillaEncoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.nio.ByteBuffer;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;

public class DeltaGorillaEncoderTest {
    private static int ROW_NUM = 10000;

    private static int REAPEAT_NUM = 100;
    private final long BASIC_FACTOR = 1l << 32;
    PublicBAOS out;
    private DeltaGorillaEncoder writer;
    private DeltaGorillaDecoder reader;
    private DeltaGorillaSorter sorter;
    private Random ran = new Random();
    private ByteBuffer buffer;

    @Before
    public void test() {
        writer = new DeltaGorillaEncoder();
        reader = new DeltaGorillaDecoder();
    }

    @Test
    public void test2() throws IOException {
        reader.reset();
        long[] data = new long[4];
        data[0] = 1000;
        data[1] = 12000;
        data[2] = 13000;
        data[3] = 11000;
        shouldReadAndWrite(data, 4);

    }

    @Test
    public void test4() throws IOException {
        // 测试乱序数据的正确性
        reader.reset();
        int datalen = 106;
        long[] data = new long[datalen];
        data[0] = 1;
        for (int i=1; i<datalen; i++){
            data[i] = ran.nextInt(3000);
        }
        writeDataToTXT(data);
        shouldReadWriteAndSort(data, datalen);
    }

    @Test
    public void testSort() throws IOException {
        reader.reset();
        long[] data = new long[5];
        data[0] = 1;
        data[1] = 1791;
        data[2] = 4;
        data[3] = 1536;
        data[4] = 2264;
        shouldReadWriteAndSort(data, 5);
        // 1,1586,1427,460,
        // 1,707,1211,397,2044,
        // 1,1791,4,1536,2264,
    }

    @Test
    public void testCSV() throws IOException {
        reader.reset();
        List<Long> times = new ArrayList<>();
        readCSV("D:\\senior\\SRT\\python_operator\\trans_data\\CS-Sensors\\test.csv", 2, ROW_NUM+1, 1,times);
        long[] data = times.stream().mapToLong(Long::longValue).toArray();
        shouldReadAndWrite(data, 1000);
    }

    @Test
    public void testCSV2() throws IOException {
        reader.reset();
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        long[] data = times.stream().mapToLong(Long::longValue).toArray();
        shouldReadWriteAndSort(data, ROW_NUM);
    }

    @Test
    public void testCSVTime() throws IOException {
        reader.reset();
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        long[] data = times.stream().mapToLong(Long::longValue).toArray();

        final long startTime = System.currentTimeMillis();
        for (int i=0; i<REAPEAT_NUM; i++){
            reader.reset();
            writer.reset();
            shouldReadWriteAndSort(data, ROW_NUM);
        }
        final long endTime = System.currentTimeMillis();
        writeDataToTXT(new long[]{ (endTime - startTime)});
    }

    @Test
    public void testBasic() throws IOException {
        reader.reset();
        long[] data = new long[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = i * i * BASIC_FACTOR;
        }
        shouldReadAndWrite(data, ROW_NUM);
    }

    @Test
    public void testBoundInt() throws IOException {
        reader.reset();
        long[] data = new long[ROW_NUM];
        for (int i = 2; i < 21; i++) {
            boundInt(i, data);
        }
    }

    private void boundInt(int power, long[] data) throws IOException {
        reader.reset();
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = ran.nextInt((int) Math.pow(2, power)) * BASIC_FACTOR;
        }
        shouldReadAndWrite(data, ROW_NUM);
    }

    @Test
    public void testRandom() throws IOException {
        reader.reset();
        long[] data = new long[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = ran.nextLong();
        }
        shouldReadAndWrite(data, ROW_NUM);
    }

    @Test
    public void testMaxMin() throws IOException {
        reader.reset();
        long[] data = new long[ROW_NUM];
        for (int i = 0; i < ROW_NUM; i++) {
            data[i] = (i & 1) == 0 ? Long.MAX_VALUE : Long.MIN_VALUE;
        }
        shouldReadAndWrite(data, ROW_NUM);
    }

    @Test
    public void testRegularEncoding() throws IOException {
        reader.reset();
        List<String> dates = getBetweenDate("1970-01-08", "1978-01-08");

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        ROW_NUM = dates.size();

        long[] data = new long[ROW_NUM];
        for (int i = 0; i < dates.size(); i++) {
            try {
                Date date = dateFormat.parse(dates.get(i));
                data[i] = date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        shouldReadAndWrite(data, ROW_NUM);
    }

    @Test
    public void testRegularWithMissingPoints() throws IOException {
        reader.reset();
        List<String> dates = getBetweenDate("1970-01-08", "1978-01-08");

        DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

        int kong = 0;
        for (int i = 0; i < dates.size(); i++) {
            if (i % 500 == 0) {
                kong++;
            }
        }

        ROW_NUM = dates.size() - kong;

        long[] data = new long[ROW_NUM];
        int j = 0;
        for (int i = 0; i < dates.size(); i++) {
            if (i % 500 == 0) {
                continue;
            }

            try {
                Date date = dateFormat.parse(dates.get(i));
                data[j++] = date.getTime();
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }

        shouldReadAndWrite(data, ROW_NUM);
    }

    private List<String> getBetweenDate(String start, String end) {
        List<String> list = new ArrayList<>();
        LocalDate startDate = LocalDate.parse(start);
        LocalDate endDate = LocalDate.parse(end);

        long distance = ChronoUnit.DAYS.between(startDate, endDate);
        if (distance < 1) {
            return list;
        }
        Stream.iterate(
                        startDate,
                        d -> {
                            return d.plusDays(1);
                        })
                .limit(distance + 1)
                .forEach(
                        f -> {
                            list.add(f.toString());
                        });
        return list;
    }

    private void writeData(long[] data, int length) {
        for (int i = 0; i < length; i++) {
            writer.encode(data[i], out);
        }
        writer.flush(out);
    }

    private void shouldReadAndWrite(long[] data, int length) throws IOException {
        out = new PublicBAOS();
        writeData(data, length);
        byte[] page = out.toByteArray();
        buffer = ByteBuffer.wrap(page);
        int i = 0;
        while (reader.hasNext(buffer)) {
            assertEquals(data[i++], reader.readLong(buffer));
        }
    }

    private void shouldReadWriteAndSort(long[] data, int length) throws IOException {
        out = new PublicBAOS();
        writeData(data, length);
        sorter = new DeltaGorillaSorter(out);
        sorter.sort();
//        byte[] page = out.toByteArray();
//        buffer = ByteBuffer.wrap(page);
//        int i = 0;
//        Arrays.sort(data);
//        while (reader.hasNext(buffer)) {
//            assertEquals(data[i++], reader.readLong(buffer));
//        }
    }

    public void prepareData(List<Long> times, List<Long> values) {
        //从共享单车数据集中加载数据
        //readCSVTime("D:\\senior\\DQ\\research\\new_encode\\data\\2013-citibike-tripdata\\2013-citibike-tripdata\\201306-citibike-tripdata.csv",2, ROW_NUM+1, 1, times);
        //readCSV("D:\\senior\\毕设\\data\\s-10_1e7_div_10_cleaned.csv", 1, ROW_NUM, 0, times);
        readCSV("D:\\senior\\毕设\\data\\201306-citibike-tripdata_digital_1000_cleaned.csv", 1, ROW_NUM, 0, times);
        readCSV("D:\\senior\\DQ\\research\\new_encode\\data\\2013-citibike-tripdata\\2013-citibike-tripdata\\201306-citibike-tripdata.csv",2, ROW_NUM+1, 3, values);
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
    public void writeDataToTXT(long[] data) {
        String filePath = "D:\\senior\\毕设\\画图\\实验章节\\不同排序算法的耗时情况\\原地排序.txt";
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
