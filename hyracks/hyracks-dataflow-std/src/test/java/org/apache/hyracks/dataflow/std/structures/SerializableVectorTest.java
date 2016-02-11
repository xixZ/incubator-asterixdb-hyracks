/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


package org.apache.hyracks.dataflow.std.structures;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.control.nc.resources.memory.FrameManager;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SerializableVectorTest {
    private FrameManager frameManager;
    private final int defaultFrameSize = 128 * 1024;
    class Pointer implements IResetableSerializable<Pointer> {
        int i;
        int j;
        public Pointer() {
            i = 0;
            j = 0;
        }

        public Pointer(int i, int j) {
            this.i = i;
            this.j = 1;
        }

        @Override
        public void reset(Pointer other) {
            i = other.i;
            j = other.j;
        }

        @Override
        public void serialize(byte[] bytes, int offset){
            writeInt(bytes, offset, i);
            writeInt(bytes, offset + 4, j);
        }

        @Override
        public void deserialize(byte[] bytes, int offset, int length){
            i = readInt(bytes, offset);
            j = readInt(bytes, offset + 4);
        }

        /**
         * write int value to bytes[offset] ~ bytes[offset+3]
         * @param bytes
         * @param offset
         * @param value
         */
        void writeInt(byte[] bytes, int offset, int value) {
            int byteIdx = offset;
            bytes[byteIdx++] = (byte) (value >> 24);
            bytes[byteIdx++] = (byte) (value >> 16);
            bytes[byteIdx++] = (byte) (value >> 8);
            bytes[byteIdx] = (byte) (value);
        }

        int readInt(byte[] bytes, int offset) {
            return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16) + ((bytes[offset + 2] & 0xff) << 8)
                    + ((bytes[offset + 3] & 0xff) << 0);
        }
    }

    /**
     * A pointer of size 16
     */
    class Pointer16 extends Pointer{
        int m, n;
        public Pointer16(int i, int j, int m, int n){
            this.i = i; this.j = j; this.m = m; this.n = n;
        }
        @Override
        public void serialize(byte[] bytes, int offset){
            writeInt(bytes, offset, i);
            writeInt(bytes, offset + 4, j);
            writeInt(bytes, offset + 8, m);
            writeInt(bytes, offset + 12, n);
        }

        @Override
        public void deserialize(byte[] bytes, int offset, int length){
            i = readInt(bytes, offset);
            j = readInt(bytes, offset + 4);
            m = readInt(bytes, offset + 8);
            n = readInt(bytes, offset + 12);
        }
    }
    @Test
    public void testInit(){
        int recordSize = 8;
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        assertEquals(0, sVector.size());
        assertEquals(0, sVector.getFrameCount());
    }

    private void testAppendHelper(int vSize, int frameSize, int recordSize) throws HyracksDataException {
        frameManager = new FrameManager(frameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for(int i = 0; i < vSize; i ++){
            sVector.append(new Pointer(i, i + 2));
            assertEquals(i+1, sVector.size());
        }
        int frameCount = calculateFrameCount(vSize, frameSize, recordSize);
        assertEquals(frameCount, sVector.getFrameCount());
    }

    @Test
    public void testAppendSmallSize() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 10000;
        testAppendHelper(vSize, defaultFrameSize, recordSize);
    }

    @Test
    public void testAppendLargeSize() throws HyracksDataException{
        int recordSize = 8;
        int vSize = 10000000;    //10M
        testAppendHelper(vSize, defaultFrameSize, recordSize);
    }

    @Test
    public void testAppendLargerSize() throws HyracksDataException{
        int recordSize = 8;
        int vSize = 100000000;  //100M
        testAppendHelper(vSize, defaultFrameSize, recordSize);
    }

    @Test
    public void testAppendNonDefaultFrameSize() throws HyracksDataException {
        int recordSize = 8;
        int frameSize = 10000;
        int vSize = 10000000;    //10M
        testAppendHelper(vSize, frameSize, recordSize);
    }
    private void testGetMethodHelper(int frameSize, int recordSize) throws HyracksDataException {
        int vSize = 1000000;    //1M
        frameManager = new FrameManager(frameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        if(recordSize == 8) {
            for (int i = 0; i < vSize; i++) {
                sVector.append(new Pointer(i, i + 2));
            }

            Pointer record = new Pointer();
            for (int i = 0; i < vSize; i++) {
                sVector.get(i, record);
                assertEquals(record, new Pointer(i, i + 2));
            }
        }
        else if(recordSize == 16){
            for (int i = 0; i < vSize; i++) {
                sVector.append(new Pointer16(i, i + 1, i + 2, i + 3));
            }

            Pointer record = new Pointer();
            for (int i = 0; i < vSize; i++) {
                sVector.get(i, record);
                assertEquals(record, new Pointer16(i, i + 1, i + 2, i + 3));
            }
        }
    }

    @Test
    public void testGetMethod1() throws HyracksDataException {
        int recordSize = 8;
        testGetMethodHelper(defaultFrameSize, recordSize);
    }

    @Test
    public void testGetMethod2() throws HyracksDataException{
        int frameSize = 100000;
        int recordSize = 16;
        testGetMethodHelper(frameSize, recordSize);
    }

    @Test
    public void testGetMethod3() throws HyracksDataException{
        int frameSize = 32 * 1024;
        int recordSize = 16;
        testGetMethodHelper(frameSize, recordSize);
    }

    @Test
    public void testGetMethodOutOfBound() throws HyracksDataException{
        int recordSize = 8;
        int vSize = 1000000;    //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for(int i = 0; i < vSize; i ++){
            sVector.append(new Pointer(i, i + 2));
        }

        Pointer record = new Pointer();
        for(int i = vSize; i < vSize + 100; i ++) {
            sVector.get(vSize, record);
        }
    }

    @Test
    public void testSetMethod() throws HyracksDataException{
        int recordSize = 8;
        int vSize = 1000000;    //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for(int i = 0; i < vSize; i ++){
            sVector.append(new Pointer(i, i + 2));
        }

        Pointer record = new Pointer();
        for(int i = 0; i < vSize; i += 5)
            sVector.set(i, new Pointer(i + 5, i + 6));

        for(int i = 0; i < vSize; i ++){
            sVector.get(i, record);
            if(i % 5 == 0)
                assertEquals(new Pointer(i + 5, i + 6), record);
            else
                assertEquals(new Pointer(i, i + 2), record);
        }

    }

    @Test
    public void testSetMethodOutOfBound() throws HyracksDataException{
        int recordSize = 8;
        int vSize = 1000000;    //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for(int i = 0; i < vSize; i ++){
            sVector.append(new Pointer(i, i + 2));
        }

        Pointer record = new Pointer();
        for(int i = vSize; i < vSize + 100; i ++) {
            sVector.set(vSize, record);
        }
    }

    @Test
    public void testClearMethod() throws HyracksDataException{
        int recordSize = 8;
        int vSize = 1000000;    //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for(int i = 0; i < vSize; i ++){
            sVector.append(new Pointer(i, i + 2));
        }

        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);

        for(int i = 0; i < vSize; i ++){
            sVector.append(new Pointer(i, i + 2));
        }
        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);
    }

    private int calculateFrameCount(int vSize, int recordSize, int frameSize){
        return (int)Math.ceil((double)vSize / (frameSize / recordSize));
    }

}