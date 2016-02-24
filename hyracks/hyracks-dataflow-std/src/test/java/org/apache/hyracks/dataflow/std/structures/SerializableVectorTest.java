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

    class RecordPointer implements IResetableSerializable<RecordPointer> {
        int[] nums;

        public RecordPointer(int size) { //size must be times of 4
            nums = new int[size / 4];
        }

        public RecordPointer(int field1, int field2) {
            nums = new int[2];
            nums[0] = field1;
            nums[1] = field2;
        }

        public RecordPointer(int field1, int field2, int field3, int field4) {
            nums = new int[4];
            nums[0] = field1;
            nums[1] = field2;
            nums[2] = field3;
            nums[3] = field4;
        }

        public void setNums(int index, int value) {
            nums[index] = value;
        }

        public int getNums(int index) {
            return nums[index];
        }

        @Override
        public void reset(RecordPointer other) {
            nums = other.nums;
        }

        @Override
        public void serialize(byte[] bytes, int offset) {
            for (int i = 0; i < nums.length; i++)
                writeInt(bytes, offset + 4 * i, nums[i]);
        }

        @Override
        public void deserialize(byte[] bytes, int offset, int length) {
            for (int i = 0; i < nums.length; i++)
                nums[i] = readInt(bytes, offset + 4 * i);
        }

        /**
         * write int value to bytes[offset] ~ bytes[offset+3]
         * 
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
            return ((bytes[offset] & 0xff) << 24) + ((bytes[offset + 1] & 0xff) << 16)
                    + ((bytes[offset + 2] & 0xff) << 8) + ((bytes[offset + 3] & 0xff) << 0);
        }
    }

    @Test
    public void testInit() {
        int recordSize = 8;
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        assertEquals(0, sVector.size());
        assertEquals(0, sVector.getFrameCount());
    }

    private void testAppendHelper(int vSize, int frameSize, int recordSize) throws HyracksDataException {
        frameManager = new FrameManager(frameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for (int i = 0; i < vSize; i++) {
            sVector.append(new RecordPointer(i, i + 2));
            assertEquals(i + 1, sVector.size());
        }
        int frameCount = calculateFrameCount(vSize, recordSize, frameSize);
        assertEquals(frameCount, sVector.getFrameCount());
    }

    @Test
    public void testAppendSmallSize() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 10000;
        testAppendHelper(vSize, defaultFrameSize, recordSize);
    }

    @Test
    public void testAppendLargeSize() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 10000000; //10M
        testAppendHelper(vSize, defaultFrameSize, recordSize);
    }

    @Test
    public void testAppendLargerSize() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 100000000; //100M
        testAppendHelper(vSize, defaultFrameSize, recordSize);
    }

    @Test
    public void testAppendNonDefaultFrameSize() throws HyracksDataException {
        int recordSize = 8;
        int frameSize = 10000;
        int vSize = 10000000; //10M
        testAppendHelper(vSize, frameSize, recordSize);
    }

    private void testGetMethodHelper(int frameSize, int recordSize) throws HyracksDataException {
        int vSize = 1000000; //1M
        frameManager = new FrameManager(frameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        RecordPointer recordPointer;
        for (int i = 0; i < vSize; i++) {
            recordPointer = new RecordPointer(recordSize);
            for (int j = 0; j < recordSize / 4; j++)
                recordPointer.setNums(j, i + j);
            sVector.append(recordPointer);
        }

        recordPointer = new RecordPointer(recordSize);
        for (int i = 0; i < vSize; i++) {
            sVector.get(i, recordPointer);
            for (int j = 0; j < recordSize / 4; j++)
                assertEquals(i + j, recordPointer.getNums(j));
        }
    }

    @Test
    public void testGetMethod1() throws HyracksDataException {
        int recordSize = 8;
        testGetMethodHelper(defaultFrameSize, recordSize);
    }

    @Test
    public void testGetMethod2() throws HyracksDataException {
        int frameSize = 100000;
        int recordSize = 16;
        testGetMethodHelper(frameSize, recordSize);
    }

    @Test
    public void testGetMethod3() throws HyracksDataException {
        int frameSize = 32 * 1024;
        int recordSize = 16;
        testGetMethodHelper(frameSize, recordSize);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testGetMethodOutOfBound() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 1000000; //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for (int i = 0; i < vSize; i++) {
            sVector.append(new RecordPointer(i, i + 2));
        }

        RecordPointer record = new RecordPointer(recordSize);
        for (int i = vSize; i < vSize + 100; i++) {
            sVector.get(vSize, record);
        }
    }

    @Test
    public void testSetMethod() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 1000000; //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for (int i = 0; i < vSize; i++) {
            sVector.append(new RecordPointer(i, i + 2));
        }

        RecordPointer record = new RecordPointer(recordSize);
        for (int i = 0; i < vSize; i += 5)
            sVector.set(i, new RecordPointer(i + 5, i + 6));

        for (int i = 0; i < vSize; i++) {
            sVector.get(i, record);
            if (i % 5 == 0) {
                assertEquals(i + 5, record.getNums(0));
                assertEquals(i + 6, record.getNums(1));
            } else {
                assertEquals(i, record.getNums(0));
                assertEquals(i + 2, record.getNums(1));
            }
        }

    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testSetMethodOutOfBound() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 1000000; //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for (int i = 0; i < vSize; i++) {
            sVector.append(new RecordPointer(i, i + 2));
        }

        RecordPointer record = new RecordPointer(recordSize);
        for (int i = vSize; i < vSize + 100; i++) {
            sVector.set(vSize, record);
        }
    }

    @Test
    public void testClearMethod() throws HyracksDataException {
        int recordSize = 8;
        int vSize = 1000000; //1M
        frameManager = new FrameManager(defaultFrameSize);
        SerializableVector sVector = new SerializableVector(frameManager, recordSize);
        for (int i = 0; i < vSize; i++) {
            sVector.append(new RecordPointer(i, i + 2));
        }

        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);

        for (int i = 0; i < vSize; i++) {
            sVector.append(new RecordPointer(i, i + 2));
        }
        sVector.clear();
        assertEquals(sVector.size(), 0);
        assertEquals(sVector.getFrameCount(), 0);
    }

    private int calculateFrameCount(int vSize, int recordSize, int frameSize) {
        return (int) Math.ceil((double) vSize / (frameSize / recordSize));
    }

}