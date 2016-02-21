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
        int m = offset + (length >> 1);

        int a = offset;
        int b = a;
        int c = offset + length - 1;
        int d = c;
        while (true) {
            while (b <= c) {
                int cmp = compare(tPointerVector, b, m);
                if (cmp > 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointerVector, a++, b);
                }
                ++b;
            }
            while (c >= b) {
                int cmp = compare(tPointerVector, c, m);
                if (cmp < 0) {
                    break;
                }
                if (cmp == 0) {
                    swap(tPointerVector, c, d--);
                }
                --c;
            }
            if (b > c)
                break;
            swap(tPointerVector, b++, c--);
        }

        int s;
        int n = offset + length;
        s = Math.min(a - offset, b - a);
        vecswap(tPointerVector, offset, b - s, s);
        s = Math.min(d - c, n - d - 1);
        vecswap(tPointerVector, b, n - s, s);

        if ((s = b - a) > 1) {
            sort(tPointerVector, offset, s);
        }
        if ((s = d - c) > 1) {
            sort(tPointerVector, n - s, s);
        }
    }

    //swap tPointerVector[a] and tPointerVector[b]
    private void swap(SerializableVector tPointerVector, int index1, int index2) {
        TPointer tmp1 = new TPointer();
        TPointer tmp2 = new TPointer();
        tPointerVector.get(index1, tmp1);
        tPointerVector.get(index2, tmp2);
        tPointerVector.set(index1, tmp2);
        tPointerVector.set(index2, tmp1);
    }

    private void vecswap(SerializableVector tPointerVector, int a, int b, int n) {
        for (int i = 0; i < n; i++, a++, b++) {
            swap(tPointerVector, a, b);
        }
    }

    //compare tPointerVector[index1], tPointerVector[index2]
    private int compare(SerializableVector tPointerVector, int index1, int index2) throws HyracksDataException{
        TPointer tPointer1 = new TPointer();
        TPointer tPointer2 = new TPointer();
        tPointerVector.get(index1, tPointer1);
        tPointerVector.get(index2, tPointer2);
        int v1 = tPointer1.id_normal_key;
        int v2 = tPointer2.id_normal_key;
        if(v1 != v2){
            return ((((long) v1) & 0xffffffffL) < (((long) v2) & 0xffffffffL)) ? -1 : 1;
        }

        int id_frameID1 = tPointer1.id_frameID;
        int id_frameID2 = tPointer2.id_frameID;

        int id_tuple_start1 = tPointer1.id_tuple_start;
        int id_tuple_start2 = tPointer2.id_tuple_start;

        ByteBuffer buf1 = super.bufferManager.getFrame(id_frameID1);
        ByteBuffer buf2 = super.bufferManager.getFrame(id_frameID2);

        byte[] b1 = buf1.array();
        byte[] b2 = buf2.array();
        inputTupleAccessor.reset(buf1);
        fta2.reset(buf2);
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : buf1.getInt(id_tuple_start1 + (fIdx - 1) * 4);
            int f1End = buf1.getInt(id_tuple_start1 + fIdx * 4);
            int s1 = id_tuple_start1 + inputTupleAccessor.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : buf2.getInt(id_tuple_start2 + (fIdx - 1) * 4);
            int f2End = buf2.getInt(id_tuple_start2 + fIdx * 4);
            int s2 = id_tuple_start2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

}