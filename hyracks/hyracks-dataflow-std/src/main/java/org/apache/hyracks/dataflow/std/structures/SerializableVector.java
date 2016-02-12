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


import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.apache.hyracks.api.context.IHyracksFrameMgrContext;
import org.apache.hyracks.api.exceptions.HyracksDataException;

public class SerializableVector implements ISerializableVector<IResetableSerializable>{

    private final int frameSize;
    private final int recordPerFrame;
    private final IHyracksFrameMgrContext ctx;
    private ArrayList<ByteBuffer> frames;
    private int recordSize;
    private int sVectorSize;
    private int frameCount;
    private int lastPos;

    /**
     * constructor
     * default frameSize is the same as system setting
     * @param recordSize
     */
    public SerializableVector(IHyracksFrameMgrContext ctx, int recordSize){
        frames = new ArrayList<>();
        int frameSize = ctx.getInitialFrameSize();
        this.frameSize = frameSize;
        this.recordSize = recordSize;
        this.ctx = ctx;
        recordPerFrame = frameSize / recordSize;
        sVectorSize = 0;
        frameCount = 0;
        lastPos = 0;
    }


    @Override
    public void get(int index, IResetableSerializable record) {
        if(index >= sVectorSize) {
            throw new IndexOutOfBoundsException("index: " + index + " current vector size: " + sVectorSize);
        }
        int frameIdx = getFrameIdx(index);
        int offsetInFrame = getOffsetInFrame(index);
        record.deserialize(frames.get(frameIdx).array(), offsetInFrame, recordSize);
    }

    @Override
    public void append(IResetableSerializable record) throws HyracksDataException {
        if(sVectorSize % recordPerFrame == 0){    //add a new frame
            ByteBuffer frame = null;
            frame = ctx.allocateFrame(frameSize);
            frameCount ++;
            record.serialize(frame.array(), 0);
            frames.add(frame);
            lastPos = recordSize;
        }
        else{
            int frameIdx = frameCount - 1;
            int offsetInFrame = lastPos;
            record.serialize(frames.get(frameIdx).array(), offsetInFrame);
            lastPos += recordSize;
        }
        sVectorSize ++;
    }

    @Override
    public void set(int index, IResetableSerializable record) {
        if(index >= sVectorSize){
            throw new IndexOutOfBoundsException("index: " + index + " current vector size: " + sVectorSize);
        }
        int frameIdx = getFrameIdx(index);
        int offsetInFrame = getOffsetInFrame(index);
        record.deserialize(frames.get(frameIdx).array(), offsetInFrame, recordSize);
    }

    @Override
    public void clear() {
        frames.clear();
        sVectorSize = 0;
        frameCount = 0;
        lastPos = 0;
    }

    @Override
    public int size() {
        return sVectorSize;
    }

    @Override
    public int getFrameCount() {
        return frameCount;
    }

    private int getFrameIdx(int index){
        return index / recordPerFrame;
    }
    private int getOffsetInFrame(int index){
        return (index % recordPerFrame) * recordSize;
    }
}