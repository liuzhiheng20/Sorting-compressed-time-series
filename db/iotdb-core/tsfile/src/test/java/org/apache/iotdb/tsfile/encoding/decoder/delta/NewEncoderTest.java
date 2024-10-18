package org.apache.iotdb.tsfile.encoding.decoder.delta;

import org.junit.Test;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class NewEncoderTest {
    // 用来测试新的编码算法的性能

    private int ROW_NUM = 1000;

    @Test
    public void testSize() throws IOException {
        // 测试按照新给出的编码算法编码数据后的空间占用情况
        List<Long> times = new ArrayList<>();
        List<Long> values = new ArrayList<>();
        prepareData(times, values);
        long[] data = times.stream().mapToLong(Long::longValue).toArray();
        for(int i=0; i<ROW_NUM; i++){
            data[i] = data[i] / 100;
        }
        // Arrays.sort(data);
        //writeDataToTXT(new long[]{encoder1(data, 1000)});

        writeDataToTXT(new long[]{encoder1(data, 1000)});
    }

    private int encoder1(long[] data, int datalen){
        // delta+分类编码
        // 返回编码数据所需要的比特长度
        int res = 0;
        int maxdata = 0;  // 记录正数所使用的最大编码长度
        long[] delta = new long[datalen-1];
        for(int i=0; i<datalen-1;i++){
            delta[i] = data[i+1] - data[i];
        }
        res += GorrilaTimeEncoder(data[0]);
        for(int i=0; i<datalen-1;i++) {
            if (delta[i]>=0){
                // 编码正数
                res += GorrilaTimeEncoder(delta[i]);
                if (maxdata < GorrilaTimeEncoder(delta[i])){
                    maxdata = GorrilaTimeEncoder(delta[i]);
                }
            } else {
                // 编码负数
                res+=36;
//                int temp = GorrilaTimeEncoder(-delta[i]);
//                if(temp<maxdata){
//                    res += maxdata;
//                } else {
//                    if(temp==1)  res+=9;
//                    if(temp==9)  res+=12;
//                    if(temp==12)  res+=16;
//                    if(temp>=16)  res+=36;
//                }
            }
        }
        return res;
    }

    private int GorrilaTimeEncoder(long delta) {
        if(delta==0){
            return 1;  // 仅使用标志位就能编码
        }
        if(delta < 128){
            return 2+7;
        }
        if(delta < 512){
            return 3+9;
        }
        if(delta < 4096){
            return 4+12;
        }
        return 4+32;
    }

    private int VarintTimeEncoder(long delta) {
        int res=8;
        long temp = 128;
        while (delta >= temp) {
            temp = temp*128;
            res += 8;
        }
        return res;
    }

    private int encoder2(long[] data, int datalen){
        // delta+varint编码
        // 返回编码数据所需要的比特长度
        // delta+分类编码
        // 返回编码数据所需要的比特长度
        int res = 0;
        int maxdata = 0;  // 记录正数所使用的最大编码长度
        long[] delta = new long[datalen-1];
        for(int i=0; i<datalen-1;i++){
            delta[i] = data[i+1] - data[i];
        }
        res += VarintTimeEncoder(data[0]);
        for(int i=0; i<datalen-1;i++) {
            if (delta[i]>=0){
                // 编码正数
                res += VarintTimeEncoder(delta[i]);
                if (maxdata < VarintTimeEncoder(delta[i])){
                    maxdata = VarintTimeEncoder(delta[i]);
                }
            } else {
                // 编码负数
                int temp = VarintTimeEncoder(-delta[i]);
                if(temp<maxdata){
                    res += maxdata;
                } else {
                    res = res+temp+8;
                }
            }
        }
        return res;
    }

    public void prepareData(List<Long> times, List<Long> values) {
        //第一种方式，从指定文件中读取数据
//        readCSV("D:\\senior\\SRT\\python_operator\\trans_data\\CS-Sensors\\test.csv",2, ROW_NUM+1, 0, times);
//        readCSV("D:\\senior\\SRT\\python_operator\\trans_data\\CS-Sensors\\test.csv",2, ROW_NUM+1, 1, values);

        //第二种方式，根据系统时间生成数据
//        for(int i=0; i<ROW_NUM;i++) {
//            times.add(System.nanoTime());
//            values.add(System.currentTimeMillis());
//        }

        //从共享单车数据集中加载数据
        readCSVTime("D:\\senior\\DQ\\research\\new_encode\\data\\2013-citibike-tripdata\\2013-citibike-tripdata\\201306-citibike-tripdata_new.csv",2, ROW_NUM+1, 1, times);
        readCSV("D:\\senior\\DQ\\research\\new_encode\\data\\2013-citibike-tripdata\\2013-citibike-tripdata\\201306-citibike-tripdata_new.csv",2, ROW_NUM+1, 3, values);
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
        String filePath = "D:\\senior\\DQ\\research\\new_encode\\data.txt";

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
