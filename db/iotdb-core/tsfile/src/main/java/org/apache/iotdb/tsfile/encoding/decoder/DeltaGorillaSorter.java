package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.DeltaDeltaLongEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaGorillaEncoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.BufferedWriter;
import java.io.ByteArrayOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Objects;

public class DeltaGorillaSorter {  // 用来实现排序算法，验证排序算法的效果
    // 实现排序过程 空间不放大 排序
    DeltaGorillaDecoder reader;
    DeltaGorillaEncoder writer;
    int lastPos;
    int nowPos;
    long lastValue;
    long nowValue;
    long valueBeforeFirstUnorderValue;
    long firstUnorderValue;
    int firstUnorderValuePos;
    PublicBAOS data;
    int totalDataLen;

    public DeltaGorillaSorter(PublicBAOS data) {
        this.data = data;
        reader = new DeltaGorillaDecoder();
        writer = new DeltaGorillaEncoder();
        lastPos = 0;
        nowPos = 0;
        firstUnorderValue = 0;
        totalDataLen = data.size()*8;
        lastValue = 0;
    }

    private void reset() {
        reader.reset();
        writer.reset();
        lastPos = 0;
        nowPos = 0;
        firstUnorderValue = 0;
        // totalDataLen = data.size()*8;
        lastValue = 0;
    }

    public void sort() {
         // 实现内存不放大的
         while (findFirstUnorderedValue()){
             //writeDataToCSV(new long[]{totalDataLen});
             findInsrtPos();
             moveUnorderedPart();
             // 测试，解码查看排序效果
//             reader.reset();
//             byte[] page = data.getBuf();
//             ByteBuffer buffer = ByteBuffer.wrap(page);
//             while (reader.hasNext(buffer)) {
//                 nowValue = reader.readLong(buffer);
//             }
             reset();
         }
    }

    public boolean findFirstUnorderedValue() {
        // 找到第一个乱序数据
        reader.reset();
        byte[] page = data.getBuf();
        ByteBuffer buffer = ByteBuffer.wrap(page);
        lastPos = 0;
        nowValue = 0;
        lastValue = 0;
        reader.reset();
        while (reader.hasNext(buffer)) {
            nowValue = reader.readLong(buffer);
            if(nowValue < lastValue) {
                // 乱序数据
                valueBeforeFirstUnorderValue = lastValue;
                firstUnorderValue = nowValue;
                firstUnorderValuePos = nowPos;
                return true;
            }
            lastValue = nowValue;
            nowPos = buffer.position()*8- reader.getBitsLeft();
        }
        return false;
    }

    public void findInsrtPos() {
        // 确定乱序数据应该插入的位置
        // nowPos代表插入位置，lastValue为之前的值，nowValue为之后的值
        byte[] page = data.getBuf();
        ByteBuffer buffer = ByteBuffer.wrap(page);
        reader.reset();
        nowPos = 0;
        long valueTemp = 0;
        while (valueTemp<firstUnorderValue){
            nowPos = buffer.position()*8- reader.getBitsLeft();
            lastValue = valueTemp;
            reader.hasNext(buffer);
            valueTemp = reader.readLong(buffer);
        }
        nowValue = valueTemp;
    }

    public void moveUnorderedPart() {
        // 将乱序部分的数据 分次移动进
        byte[] page = data.getBuf();
        ByteBuffer buffer = ByteBuffer.wrap(page);
        readerReset(firstUnorderValuePos, buffer);
        reader.hasNext(buffer);
        long valueBeforeMinUnorderedValue = firstUnorderValue - reader.lastDelta;
        reader.setLastValue(valueBeforeMinUnorderedValue);
        long valuetemp = reader.readLong(buffer);
        //boolean useFixedLen = false;
        //int fixlen = 0;
        long deltaTemp = 0;
        while(valuetemp <= nowValue){
            // 删除当前value，插入当前value
            deltaTemp = valuetemp-lastValue;
            int postemp = buffer.position()*8 - reader.getBitsLeft();
            deleteValue(firstUnorderValuePos, postemp);
            firstUnorderValuePos += insertValue(valuetemp);
            readerReset(firstUnorderValuePos, buffer);
            lastValue = valuetemp;// 这里直接对data中的数据进行的修改，buffer中的数组会随之修改吗？
            if(reader.hasNext(buffer)) {
                valuetemp = reader.readLong(buffer);
            } else {
                break;
            }
        }
        // delete and insert nowvalue
        firstUnorderValuePos += deleteAndEnsertValue(nowPos, buffer, nowValue-lastValue);
        if(valuetemp > nowValue) {
            // delete and insert valuetemp
            deleteAndEnsertValue(firstUnorderValuePos,buffer,valuetemp-valueBeforeFirstUnorderValue);
        }
    }

    public void readerReset(int pos, ByteBuffer buffer) {
        int bitsleft = 8-pos%8;
        int index = (pos+bitsleft)/8;
        reader.setBuffer(buffer.get(index-1),bitsleft);
        buffer.position(index);
    }

    public void deleteValue(int startpos, int endpos){
        totalDataLen -= (endpos-startpos);
        data.leftMoveBuffer(endpos, endpos-startpos);
    }

    public int deleteAndEnsertValue(int startPos, ByteBuffer buffer, long delta) {
        // 在startPos位置，删除一个元素的同时插入一个元素
        // 返回序列右移的长度
        readerReset(startPos, buffer);
        writer.reset();
        int deletelen = reader.nextLen(buffer);
        data.leftMoveBuffer(startPos+deletelen, deletelen);
        int insertlen = writer.GorrilaTimeEncoder(delta, false);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.compressValue(delta,insertlen, out);
        writer.flushBuffer(out);
        data.rightMoveBuffer(startPos, insertlen);
        data.writeBuffer(startPos, insertlen, out.toByteArray());
        totalDataLen += insertlen-deletelen;
        return insertlen-deletelen;
    }

    public int insertValue(long value) {
        // 在nowPos的位置插入值为value的新值
        writer.reset();
        long delta = value-this.lastValue;
        int datalen = writer.GorrilaTimeEncoder(delta,false);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.compressValue(delta,datalen, out);
        writer.flushBuffer(out);
        data.rightMoveBuffer(nowPos, datalen);
        data.writeBuffer(nowPos,datalen,out.toByteArray());
        nowPos += datalen;
        totalDataLen += datalen;
        this.lastValue = value;
        return datalen;
    }

//    public void findMinUnorderValue() {
//        // 确定最小的乱序数据
//        byte[] page = data.getBuf();
//        ByteBuffer buffer = ByteBuffer.wrap(page);
//        lastPos = nowPos;
//        nowPos = buffer.position()*8- reader.getBitsLeft();
//        while (reader.hasNext(buffer)) {
//            nowValue = reader.readLong(buffer);
//            if(nowValue < lastValue){
//                if(minUnorderValue==0 || nowValue<minUnorderValue) {
//                    minUnorderValue = nowValue;
//                    minUnorderLastPos = lastPos;
//                    minUnorderNowPos = nowPos;
//                }
//            }
//            lastValue = nowValue;
//        }
//    }
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
    public void writeDataToCSV(long[] data) {
        String filePath = "D:\\senior\\DQ\\research\\new_encode\\totalDatalen.csv";

        // 使用 try-with-resources 自动关闭资源
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filePath, true))) {
            // 检查数组是否为空
            if (data.length == 0) {
                writer.write("\n"); // 如果数组为空，只写入一个换行符
                return;
            }

            // 遍历数组并将每个元素写入文件
            for (int i = 0; i < data.length; i++) {
                writer.write(String.valueOf(data[i])); // 写入数字
                if (i < data.length - 1) { // 如果不是最后一个元素，后面添加逗号
                    writer.write(",");
                }
            }
            writer.newLine(); // 使用newLine()方法写入系统默认的换行符
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
