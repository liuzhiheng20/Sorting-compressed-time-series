package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.DeltaDeltaLongEncoder;
import org.apache.iotdb.tsfile.encoding.encoder.DoublePrecisionEncoderV2;
import org.apache.iotdb.tsfile.encoding.encoder.Encoder;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import java.io.IOException;

public class DeltaDeltaLongSorter {
    // 对时间列和数值列同时进行排序
    protected TSDataType dataType;
    private Encoder timeEncoder;
    private PublicBAOS timeOut;
    private Encoder valueEncoder;
    private PublicBAOS valueOut;
    protected Decoder valueDecoder;
    protected Decoder timeDecoder;

    protected long totalDatalen = 0; // 按照bit记录数据的长度
    int totalDataLen;

    public void all_uncompressing_sort() throws IOException {
        // 全部数据解压缩到tvlist中，在tvlist中进行排序，将排序后的数据再重新编码回压缩表示当中

    }
}
