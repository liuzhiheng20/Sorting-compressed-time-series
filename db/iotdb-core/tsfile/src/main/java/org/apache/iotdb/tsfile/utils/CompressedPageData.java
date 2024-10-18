package org.apache.iotdb.tsfile.utils;

public class CompressedPageData {
    TS_DELTA_data timeData;
    TS_DELTA_data valueData;
    long minTime;
    long maxTime;
    int pageTimeLen;
    int pageValueLen;
    boolean isInSorter;
    int count;
    public CompressedPageData(TS_DELTA_data t, TS_DELTA_data v, int totalNum, long minT, long maxT) {
        this.timeData = t;
        this.valueData = v;
        this.count = totalNum;
        this.minTime = minT;
        this.maxTime = maxT;
        this.isInSorter = true;
        this.pageTimeLen = t.getLen();
        this.pageValueLen = v.getLen();
    }

    public boolean getIsInSorter() {
        return this.isInSorter;
    }
    public long getMaxTime() {
        return this.maxTime;
    }

    public long getMinTime() {
        return this.minTime;
    }
    public int getPageTimeLen() {
        return this.pageTimeLen;
    }
    public int getPageValueLen() {
        return this.pageValueLen;
    }

    public int getCount() {
        return this.count;
    }

    public TS_DELTA_data getTimeData() {
        return this.timeData;
    }

    public TS_DELTA_data getValueData() {
        return this.valueData;
    }
}
