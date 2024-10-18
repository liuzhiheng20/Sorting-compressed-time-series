package org.apache.iotdb.tsfile.encoding.decoder.delta;

import org.apache.iotdb.tsfile.encoding.decoder.VarDeltaLongDecoder;
import org.apache.iotdb.tsfile.encoding.decoder.VarLongDecoder;
import org.apache.iotdb.tsfile.encoding.encoder.TSDeltaEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.VVarIntEncoder;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.iotdb.tsfile.utils.TS_DELTA_data;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class PersistCompressedSortEncodeTest {

    private static int ROW_NUM = 108;
    private static int REAPEAT_NUM = 1;
    private static long LOW_BOUND = -10000;
    private static long UP_BOUND = 10000;
    @Test
    public void testTimeEncoder() throws IOException {
        int[] data = new int[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            data[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
        }
        TSDeltaEncoder timeEncoder = new TSDeltaEncoder();
        TS_DELTA_data timeCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            timeEncoder.encode(data[i], timeCompressedData);
        }
        PublicBAOS timeOut = new PublicBAOS();
        ReadWriteForEncodingUtils.writeUnsignedInt(timeEncoder.getValsNum(), timeOut);
        timeOut.write(timeCompressedData.lens, 0, (timeEncoder.getValsNum()-1)/4+1);
        timeOut.write(timeCompressedData.vals, 0, timeEncoder.getValsLen());
        ByteBuffer timeBuffer = ByteBuffer.wrap(timeOut.getBuf());
        VarDeltaLongDecoder decoder = new VarDeltaLongDecoder();
        int nownum = 0;
        while(decoder.hasNext(timeBuffer)){
            assertEquals(data[nownum], decoder.readLong(timeBuffer));
            nownum++;
        }
    }

    @Test
    public void testValueEncoder() throws IOException {
        int[] data = new int[ROW_NUM];
        for(int i=0; i<ROW_NUM; i++) {
            data[i] = (int) ((Math.random() * (UP_BOUND - LOW_BOUND + 1)) + LOW_BOUND);
        }
        VVarIntEncoder valueEncoder = new VVarIntEncoder();
        TS_DELTA_data valueCompressedData = new TS_DELTA_data();
        for (int i=0; i<ROW_NUM; i++) {
            valueEncoder.encode(data[i], valueCompressedData);
        }
        PublicBAOS valueOut = new PublicBAOS();
        ReadWriteForEncodingUtils.writeUnsignedInt(valueEncoder.getValsNum()+1, valueOut);
        valueOut.write(valueCompressedData.lens, 0, (valueEncoder.getValsNum()-1)/4+1);
        valueOut.write(valueCompressedData.vals, 0, valueEncoder.getValsLen());
        ByteBuffer valueBuffer = ByteBuffer.wrap(valueOut.getBuf());
        VarLongDecoder decoder = new VarLongDecoder();
        int nownum = 0;
        while(decoder.hasNext(valueBuffer)){
            assertEquals(data[nownum], decoder.readLong(valueBuffer));
            nownum++;
        }
    }
}
