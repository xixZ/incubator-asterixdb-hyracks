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
package org.apache.hyracks.dataflow.std.sort;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.context.IHyracksTaskContext;
import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.INormalizedKeyComputerFactory;
import org.apache.hyracks.api.dataflow.value.RecordDescriptor;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.comm.io.FrameTupleAccessor;
import org.apache.hyracks.dataflow.std.sort.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.structures.SerializableVector;

public class FrameSorterQuickSort extends AbstractFrameSorter {

    private FrameTupleAccessor fta2;

    private TPointer tmpTPtr1 = new TPointer();
    private TPointer tmpTPtr2 = new TPointer();

    public FrameSorterQuickSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        this(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                Integer.MAX_VALUE);
    }

    public FrameSorterQuickSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int outputLimit) throws HyracksDataException {
        super(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                outputLimit);
        fta2 = new FrameTupleAccessor(recordDescriptor);
    }

    @Override
    void sortTupleReferences() throws HyracksDataException {
        sort(tPointerVec, 0, tupleCount);
    }

    void sort(SerializableVector tPointerVector, int offset, int length) throws HyracksDataException {
        if (length <= 1) {
            return;
        }
        int left = offset + 1, right = offset + length - 1;
        while (left <= right) {
            int cmp = compare(tPointerVector, left, offset);
            if (cmp > 0) {
                swap(tPointerVector, left, right);
                right--;
            } else {
                left++;
            }
        }
        swap(tPointerVector, offset, right);
        sort(tPointerVector, offset, right - offset);
        sort(tPointerVector, right + 1, length - 1 - (right - offset));
    }

    private void swap(SerializableVector tPointerVector, int index1, int index2) {
        tPointerVector.get(index1, tmpTPtr1);
        tPointerVector.get(index2, tmpTPtr2);
        tPointerVector.set(index1, tmpTPtr2);
        tPointerVector.set(index2, tmpTPtr1);
    }

    private int compare(SerializableVector tPointerVector, int index1, int index2) throws HyracksDataException {
        tPointerVector.get(index1, tmpTPtr1);
        tPointerVector.get(index2, tmpTPtr2);
        int v1 = tmpTPtr1.normalKey;
        int v2 = tmpTPtr2.normalKey;
        if (v1 != v2) {
            return ((((long) v1) & 0xffffffffL) < (((long) v2) & 0xffffffffL)) ? -1 : 1;
        }

        int frameID1 = tmpTPtr1.frameID;
        int frameID2 = tmpTPtr2.frameID;

        int tupleStart1 = tmpTPtr1.tupleStart;
        int tupleStart2 = tmpTPtr2.tupleStart;

        ByteBuffer buf1 = super.bufferManager.getFrame(frameID1);
        ByteBuffer buf2 = super.bufferManager.getFrame(frameID2);

        byte[] b1 = buf1.array();
        byte[] b2 = buf2.array();
        inputTupleAccessor.reset(buf1);
        fta2.reset(buf2);
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : buf1.getInt(tupleStart1 + (fIdx - 1) * 4);
            int f1End = buf1.getInt(tupleStart1 + fIdx * 4);
            int s1 = tupleStart1 + inputTupleAccessor.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : buf2.getInt(tupleStart2 + (fIdx - 1) * 4);
            int f2End = buf2.getInt(tupleStart2 + fIdx * 4);
            int s2 = tupleStart2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

}