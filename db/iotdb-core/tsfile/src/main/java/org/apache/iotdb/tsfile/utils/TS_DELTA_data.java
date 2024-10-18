package org.apache.iotdb.tsfile.utils;

public class TS_DELTA_data {
    public byte[] lens;
    public byte[] vals;

    public TS_DELTA_data() {
        lens = new byte[100];
        vals = new byte[100];
    }


    public TS_DELTA_data(byte[] v, byte[] l) {
        this.vals = v;
        this.lens = l;
    }
    public void expandLens() {
        // 创建一个新的数组，长度为 newLength
        byte[] expanded = new byte[lens.length*2];
        System.arraycopy(lens, 0, expanded, 0, lens.length);
        this.lens = expanded;
    }

    public void expandVals() {
        // 创建一个新的数组，长度为 newLength
        byte[] expanded = new byte[vals.length*2];
        System.arraycopy(vals, 0, expanded, 0, vals.length);
        this.vals = expanded;
    }

    public void checkExpand(int valsLen, int valNum, int byteNum) {
        if(valsLen+byteNum >= vals.length){
            expandVals();
        }
        if(valNum/4 >= lens.length-1){
            expandLens();
        }
    }

    public void rightMoveVals(int len, int beg, int end) {
        // 将vals数组中下标为beg到end的元素向右移动len个位置
        // 需要在调用的地方保证不溢出
        for(int i=end-1; i>=beg; i--) {
            vals[i+len] = vals[i];
        }
    }

    public void leftMoveVals(int len, int beg, int end) {
        // 将vals数组中下标为beg到end的元素向右移动len个位置
        // 需要在调用的地方保证不溢出
        for(int i=beg; i<end; i++) {
            vals[i-len] = vals[i];
        }
    }

    public int getLen() {
        return vals.length;
    }

    public void reset(int pointNum, int valsLen) {
        // 将数组进行压缩，保留其中的有效信息
        byte[] newLen = new byte[(pointNum+1)/4+1];
        System.arraycopy(lens, 0, newLen, 0, newLen.length);
        byte[] newVals = new byte[valsLen+1];
        System.arraycopy(vals, 0, newVals, 0, newVals.length);
        lens = newLen;
        vals = newVals;
    }
}
