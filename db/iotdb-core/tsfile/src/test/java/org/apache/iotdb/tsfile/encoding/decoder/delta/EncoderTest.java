package org.apache.iotdb.tsfile.encoding.decoder.delta;

import org.apache.iotdb.tsfile.encoding.decoder.DeltaDeltaLongDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaDeltaLongEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DeltaGorillaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

public class EncoderTest {
    private static int ROW_NUM = 100000;
    private DeltaGorillaEncoder D_1_writer;
    PublicBAOS D_1_out;
    private DeltaDeltaLongEncoder D_2_writer;
    PublicBAOS D_2_out;

    @Before
    public void test() {
        D_2_writer = new DeltaDeltaLongEncoder();
        D_1_writer = new DeltaGorillaEncoder();
        D_1_out = new PublicBAOS();
        D_2_out = new PublicBAOS();
    }

    @Test
    public void testEncodeSize() throws IOException {
        List<Long> times = new ArrayList<>();
        String filePath1 ="D:\\senior\\毕设\\data\\201306-citibike-tripdata_digital_1000.csv";
        String filePath2 ="D:\\senior\\毕设\\data\\s-10_1e7_div_10_.csv";
        readCSV(filePath1,1, ROW_NUM, 0, times);
        long[] data = times.stream().mapToLong(Long::longValue).toArray();
        Arrays.sort(data);
        writeData(D_1_writer, D_1_out,data);
        writeData(D_2_writer, D_2_out,data);
        filePath1 = "";
    }

    public void writeData(Encoder writer, PublicBAOS out, long[] data){
        for (int i = 0; i < ROW_NUM; i++) {
            writer.encode(data[i], out);
        }
        //writer.flush(out);
    }


    public boolean readCSV(String filePath, int line_begin, int line_end, int col, List<Long> times) {
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
