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
import org.apache.hyracks.dataflow.common.util.IntSerDeUtils;
import org.apache.hyracks.dataflow.std.sort.buffermanager.IFrameBufferManager;
import org.apache.hyracks.dataflow.std.structures.SerializableVector;

public class FrameSorterMergeSort extends AbstractFrameSorter {

    //private int[] tPointersTemp;
    private SerializableVector tPointerVecTemp;
    private FrameTupleAccessor fta2;

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor) throws HyracksDataException {
        this(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                Integer.MAX_VALUE);
    }

    public FrameSorterMergeSort(IHyracksTaskContext ctx, IFrameBufferManager bufferManager, int[] sortFields,
            INormalizedKeyComputerFactory firstKeyNormalizerFactory, IBinaryComparatorFactory[] comparatorFactories,
            RecordDescriptor recordDescriptor, int outputLimit) throws HyracksDataException {
        super(ctx, bufferManager, sortFields, firstKeyNormalizerFactory, comparatorFactories, recordDescriptor,
                outputLimit);
        fta2 = new FrameTupleAccessor(recordDescriptor);
        tPointerVecTemp = new SerializableVector(ctx, RECORD_SIZE);
    }

    @Override
    void sortTupleReferences() throws HyracksDataException {
        if(tPointerVecTemp.size() < tPointerVec.size()){
            int len = tPointerVec.size();
            tPointerVecTemp.clear();
            for(int i = 0; i < len; i ++){
                TPointer tPointer = new TPointer();
                tPointerVecTemp.append(tPointer);
            }
        }
        sort(0, tupleCount);
    }

    @Override
    public void close() {
        super.close();
        tPointerVecTemp.clear();
    }

    void sort(int offset, int length) throws HyracksDataException {
        int step = 1;
        int end = offset + length;
        /** bottom-up merge */
        while (step < length) {
            /** merge */
            for (int i = offset; i < end; i += 2 * step) {
                int next = i + step;
                if (next < end) {
                    merge(i, next, step, Math.min(step, end - next));
                } else {
                    sVectorCopy(tPointerVec, i, tPointerVecTemp, i, (end - i));
                }
            }
            /** prepare next phase merge */
            step *= 2;
            SerializableVector tmp = tPointerVecTemp;
            tPointerVecTemp = tPointerVec;
            tPointerVec = tmp;
        }
    }

    /**
     * Merge two subarrays into one
     */
    private void merge(int start1, int start2, int len1, int len2) throws HyracksDataException {
        int targetPos = start1;
        int pos1 = start1;
        int pos2 = start2;
        int end1 = start1 + len1 - 1;
        int end2 = start2 + len2 - 1;
        while (pos1 <= end1 && pos2 <= end2) {
            int cmp = compare(pos1, pos2);
            if (cmp <= 0) {
                copy(pos1, targetPos);
                pos1++;
            } else {
                copy(pos2, targetPos);
                pos2++;
            }
            targetPos++;
        }
        if (pos1 <= end1) {
            int rest = end1 - pos1 + 1;
            sVectorCopy(tPointerVec, pos1, tPointerVecTemp, targetPos, rest);
        }
        if (pos2 <= end2) {
            int rest = end2 - pos2 + 1;
            sVectorCopy(tPointerVec, pos2, tPointerVecTemp, targetPos, rest);
        }
    }

    private void sVectorCopy(SerializableVector src, int srcPos, SerializableVector dst, int dstPos, int len){
        TPointer tmp = new TPointer();
        for(int i = 0; i < len; i ++){
            src.get(srcPos + i, tmp);
            dst.set(dstPos + i, tmp);
        }
    }
    //copy tPointerVec[src] to tPointerVecTemp[dest]
    private void copy(int src, int dest) {
        TPointer tmp = new TPointer();
        tPointerVec.get(src, tmp);
        tPointerVecTemp.set(dest, tmp);
    }

    private int compare(int tp1, int tp2) throws HyracksDataException {
        TPointer tmp_TPointer1 = new TPointer();
        TPointer tmp_TPointer2 = new TPointer();
        tPointerVec.get(tp1, tmp_TPointer1);
        tPointerVec.get(tp2, tmp_TPointer2);

        int i1 = tmp_TPointer1.id_frameID;
        int j1 = tmp_TPointer1.id_tuple_start;
        int v1 = tmp_TPointer1.id_normal_key;

        int tp2i = tmp_TPointer2.id_frameID;
        int tp2j = tmp_TPointer2.id_tuple_start;
        int tp2v = tmp_TPointer2.id_normal_key;

        if (v1 != tp2v) {
            return ((((long) v1) & 0xffffffffL) < (((long) tp2v) & 0xffffffffL)) ? -1 : 1;
        }
        int i2 = tp2i;
        int j2 = tp2j;
        ByteBuffer buf1 = super.bufferManager.getFrame(i1);
        ByteBuffer buf2 = super.bufferManager.getFrame(i2);
        byte[] b1 = buf1.array();
        byte[] b2 = buf2.array();
        inputTupleAccessor.reset(buf1);
        fta2.reset(buf2);
        for (int f = 0; f < comparators.length; ++f) {
            int fIdx = sortFields[f];
            int f1Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b1, j1 + (fIdx - 1) * 4);
            int f1End = IntSerDeUtils.getInt(b1, j1 + fIdx * 4);
            int s1 = j1 + inputTupleAccessor.getFieldSlotsLength() + f1Start;
            int l1 = f1End - f1Start;
            int f2Start = fIdx == 0 ? 0 : IntSerDeUtils.getInt(b2, j2 + (fIdx - 1) * 4);
            int f2End = IntSerDeUtils.getInt(b2, j2 + fIdx * 4);
            int s2 = j2 + fta2.getFieldSlotsLength() + f2Start;
            int l2 = f2End - f2Start;
            int c = comparators[f].compare(b1, s1, l1, b2, s2, l2);
            if (c != 0) {
                return c;
            }
        }
        return 0;
    }

}
