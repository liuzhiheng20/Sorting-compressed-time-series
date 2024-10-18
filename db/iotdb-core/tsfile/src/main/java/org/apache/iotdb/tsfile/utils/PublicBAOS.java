/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * A subclass extending <code>ByteArrayOutputStream</code>. It's used to return the byte array
 * directly. Note that the size of byte array is large than actual size of valid contents, thus it's
 * used cooperating with <code>size()</code> or <code>capacity = size</code>
 */
public class PublicBAOS extends ByteArrayOutputStream {

  public PublicBAOS() {
    super();
  }

  public PublicBAOS(int size) {
    super(size);
  }

  /**
   * get current all bytes data
   *
   * @return all bytes data
   */
  public byte[] getBuf() {
    return this.buf;
  }

  public void setBuf(byte[] b) {
    this.buf = b;
  }

  /**
   * It's not a thread-safe method. Override the super class's implementation. Remove the
   * synchronized key word, to save the synchronization overhead.
   *
   * <p>Writes the complete contents of this byte array output stream to the specified output stream
   * argument, as if by calling the output stream's write method using <code>
   * out.write(buf, 0, count)</code>.
   *
   * @param out the output stream to which to write the data.
   * @exception IOException if an I/O error occurs.
   */
  @Override
  @SuppressWarnings("squid:S3551")
  public void writeTo(OutputStream out) throws IOException {
    out.write(buf, 0, count);
  }

  /**
   * It's not a thread-safe method. Override the super class's implementation. Remove the
   * synchronized key word, to save the synchronization overhead.
   *
   * <p>Resets the <code>count</code> field of this byte array output stream to zero, so that all
   * currently accumulated output in the output stream is discarded. The output stream can be used
   * again, reusing the already allocated buffer space.
   */
  @Override
  @SuppressWarnings("squid:S3551")
  public void reset() {
    count = 0;
  }

  public void setCount(int c) {
    count = c;
  }

  /**
   * The synchronized keyword in this function is intentionally removed. For details, see
   * https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=173085039
   */
  @Override
  @SuppressWarnings("squid:S3551")
  public int size() {
    return count;
  }

  public void truncate(int size) {
    count = size;
  }
  public void rightMoveBuffer(int movePoint, int moveLen){
    // 从指定位置开始，向右移动指定长度
    // 首先检查buffer的长度，如果不够，则扩容
    // appendToTXT("right move start"+"\n");
    int startPos = movePoint/8;
    int startOffset = movePoint%8;
    int movePos = (moveLen)/8;
    int moveIndex = moveLen%8;
    // 保证容量
//    for(int i=0 ; i<=moveIndex; i++){
//      write(0);
//    }
    byte nowTail = 0;
    int temp = 0;

    int index = this.buf.length-1;
    while (index > startPos){
      temp = this.buf[index];
      if(temp<0){
        temp = temp+256;
      }
      nowTail= (byte) (nowTail|temp<<((8-moveIndex)));
      if(index+movePos+1 >= this.buf.length){
        index--;
        continue;
        // index=0;
      }
      this.buf[index+movePos+1] = nowTail;
      nowTail = (byte) (temp>>>moveIndex);
      index--;
    }

    if(8-startOffset-moveIndex <= 0) { // 8-startOffset+8-moveIndex <= 8
      temp = (byte) (this.buf[startPos]<<(8-moveIndex));
      temp = (byte) (temp | nowTail);
      temp = (byte) (temp & ((0x01<<(8-startOffset+8-moveIndex))-1));
      this.buf[index+movePos+1] = (byte) temp;
    }
    else {
      temp = this.buf[startPos];
      if(temp<0){
        temp = temp+256;
      }
      nowTail= (byte) (nowTail|temp<<(8-moveIndex));
      this.buf[startPos+movePos+1] = nowTail;
      nowTail = (byte) (temp>>>moveIndex);
      nowTail = (byte) (nowTail & ((0x01<<(8-startOffset-moveIndex))-1));
      temp = this.buf[startPos+movePos];
      temp = (byte) (temp & ~((0x01<<(8-startOffset-moveIndex))-1));
      this.buf[startPos+movePos] = (byte) (temp|nowTail);
    }
    // appendToTXT("right move end"+"\n");
  }

  public void leftMoveBuffer(int movePoint, int moveLen) {
    // 从指定位置开始，向左移动指定长度
    // 先把最开始的几个左移
    // 再把后面的完整的byte左移
    // appendToTXT("left move start"+"\n");
    int startPos = movePoint/8;
    int startOffset = movePoint%8;
    int movePos = moveLen/8;
    int moveIndex = moveLen%8;
    int leftStartOffset = (startOffset-moveIndex+8)%8;
    int leftStartPos = -(moveLen-startOffset-8+leftStartOffset)/8+startPos-1;
    byte leftOld = 0;
    byte nowNew = 0;
    int temp = 0;
    temp = this.buf[startPos];

    if(8-startOffset<=8-leftStartOffset) {
      // 最左边的几个能放到一个byte里面
      leftOld = (byte) (this.buf[leftStartPos] & (-256 >>> leftStartOffset));
      nowNew = (byte) ((this.buf[startPos] << (startOffset-leftStartOffset))&((1<<(8-leftStartOffset))-1));
      this.buf[leftStartPos] =  (byte)(leftOld | nowNew);
    }
    else{
      leftOld = (byte) (this.buf[leftStartPos] & (-256 >>> leftStartOffset));
      nowNew = (byte) ((this.buf[startPos] >> (leftStartOffset-startOffset))&((1<<(8-leftStartOffset))-1));
      this.buf[leftStartPos] = (byte)(leftOld | nowNew);
      nowNew = (byte) (this.buf[startPos] << (8-(leftStartOffset-startOffset)));
      this.buf[leftStartPos+1] = nowNew;
    }

    leftStartPos += (leftStartOffset+(8-startOffset))/8;
    leftStartOffset = (leftStartOffset+(8-startOffset))%8;


    int index = startPos+1;
    while (index < this.buf.length){
      temp = this.buf[index];
      if (temp<0) {
        temp = temp+256;
      }
      leftOld = (byte) (this.buf[leftStartPos] & (-256 >>> leftStartOffset));
      nowNew = (byte) (temp >> leftStartOffset);
      this.buf[leftStartPos]= (byte) (leftOld | nowNew);
      leftStartPos++;
      if(leftStartOffset != 0) {
        this.buf[leftStartPos] = (byte) (temp<<(8-leftStartOffset));
      }
      index++;
    }
    // appendToTXT("left move end"+"\n");
  }

  public void writeBuffer(int movePoint, int writeLen, byte data[]) {
    // 在buffer的指定位置，写入来自于data的指定长度的元素
    // 要求data的长度要比其实际的容量大至少1
    // appendToTXT("write start"+"\n");
    int startPos = movePoint/8;
    int startOffset = movePoint%8;
    byte temp = 0;
    byte newdata = 0;
    byte mask = 0;
    // 如果插在一个byte中
    if (startOffset+writeLen<=8){
      temp = this.buf[startPos];
      byte mask1 = (byte) ((1<<(8-startOffset-writeLen))-1);
      byte mask2 = (byte) (-256>>>startOffset);
      mask = (byte) (((1<<(8-startOffset-writeLen))-1) | (-256>>>startOffset));
      newdata = (byte) ((data[0]>>startOffset) & (~mask));
      this.buf[startPos] = (byte) ((temp&mask)|newdata);
    }
    // 如果插入会跨byte
    else {
      // 写入数据的尾部
      temp = this.buf[startPos];
      mask = (byte) (-256>>>startOffset);
      newdata = (byte) ((data[0]>>startOffset) & ((1<<(8-startOffset))-1));
      this.buf[startPos] = (byte) ((temp&mask)|newdata);
      // 写入数据的中部
      writeLen -= 8-startOffset;
      int index = 0;
      while (writeLen>=8) {
        temp = (byte) (data[index]<<(8-startOffset));
        index++;
        newdata = (byte) ((data[index]>>startOffset) & ((1<<(8-startOffset))-1));
        this.buf[startPos+index] = (byte) (temp | newdata);
        writeLen -= 8;
      }
      // 写入数据的头部
      if (writeLen>0) {
        temp = this.buf[startPos + index + 1];
        mask = (byte) ((1 << (8-writeLen))-1);
        newdata = (byte) (data[index]<<(8-startOffset));
        index++;
        if(index<data.length)
            newdata |= (byte) ((data[index]>>startOffset) & ((int)(1<<(8-startOffset))-1));
        // newdata |= (byte) ((1<<(8-startOffset))-1);
        this.buf[startPos+index] =  (byte) ((temp&mask)|(newdata&(~mask)));
      }
    }
    // appendToTXT("write end"+"\n");
  }
}
