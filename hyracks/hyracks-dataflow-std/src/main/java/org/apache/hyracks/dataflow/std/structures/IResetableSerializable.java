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

public interface IResetableSerializable<T> extends IResetable<T> {
    /**
     * Serialize data into bytes starting from offset
     *
     * @param bytes
     *            a byte array to serialize data into
     * @param offset
     *            starting offset
     */
    void serialize(byte[] bytes, int offset);

    /**
     * Deserialize data from bytes starting from offset
     *
     * @param bytes
     *            source data byte array
     * @param offset
     *            starting offset to read from
     * @param length
     *            the length of a record
     */
    void deserialize(byte[] bytes, int offset, int length);
}