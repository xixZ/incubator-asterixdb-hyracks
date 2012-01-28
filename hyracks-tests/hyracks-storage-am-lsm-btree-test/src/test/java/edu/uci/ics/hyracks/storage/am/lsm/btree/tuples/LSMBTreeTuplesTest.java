/*
 * Copyright 2009-2010 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.uci.ics.hyracks.storage.am.lsm.btree.tuples;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.Test;

import edu.uci.ics.hyracks.api.dataflow.value.ISerializerDeserializer;
import edu.uci.ics.hyracks.api.dataflow.value.ITypeTraits;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.data.marshalling.UTF8StringSerializerDeserializer;
import edu.uci.ics.hyracks.dataflow.common.util.SerdeUtils;
import edu.uci.ics.hyracks.dataflow.common.util.TupleUtils;
import edu.uci.ics.hyracks.storage.am.common.datagen.DataGenUtils;
import edu.uci.ics.hyracks.storage.am.common.datagen.IFieldValueGenerator;

@SuppressWarnings("rawtypes")
public class LSMBTreeTuplesTest {

    private final Random rnd = new Random(50);
    
    private ByteBuffer writeTuple(ITupleReference tuple, LSMBTreeTupleWriter tupleWriter) {
        // Write tuple into a buffer, then later try to read it.
        int bytesRequired = tupleWriter.bytesRequired(tuple);
        byte[] bytes = new byte[bytesRequired];
        ByteBuffer targetBuf = ByteBuffer.wrap(bytes);
        tupleWriter.writeTuple(tuple, bytes, 0);
        return targetBuf;
    }
    
    private void testLSMBTreeTuple(ISerializerDeserializer[] maxFieldSerdes) throws HyracksDataException {        
        // Create a tuple with the max-1 fields for checking setFieldCount() of tuple references later.
        ITypeTraits[] maxTypeTraits = SerdeUtils.serdesToTypeTraits(maxFieldSerdes); 
        IFieldValueGenerator[] maxFieldGens = DataGenUtils.getFieldGensFromSerdes(maxFieldSerdes, rnd, false);
        // Generate a tuple with random field values.
        Object[] maxFields = new Object[maxFieldSerdes.length];
        for (int j = 0; j < maxFieldSerdes.length; j++) {
            maxFields[j] = maxFieldGens[j].next();
        }            
        
        // Run test for varying number of fields and keys.
        for (int numKeyFields = 1; numKeyFields < maxFieldSerdes.length; numKeyFields++) {
            // Create tuples with varying number of fields, and try to interpret their bytes with the lsmBTreeTuple.
            for (int numFields = numKeyFields; numFields <= maxFieldSerdes.length; numFields++) {                
                // Create and write tuple to bytes using an LSMBTreeTupleWriter.
                LSMBTreeTupleWriter maxMatterTupleWriter = new LSMBTreeTupleWriter(maxTypeTraits, numKeyFields, false);
                ITupleReference maxTuple = TupleUtils.createTuple(maxFieldSerdes, (Object[])maxFields);
                ByteBuffer maxMatterBuf = writeTuple(maxTuple, maxMatterTupleWriter);
                // Tuple reference should work for both matter and antimatter tuples (doesn't matter which factory creates it).
                LSMBTreeTupleReference maxLsmBTreeTuple = (LSMBTreeTupleReference) maxMatterTupleWriter.createTupleReference();
                
                ISerializerDeserializer[] fieldSerdes = Arrays.copyOfRange(maxFieldSerdes, 0, numFields);
                ITypeTraits[] typeTraits = SerdeUtils.serdesToTypeTraits(fieldSerdes);                
                IFieldValueGenerator[] fieldGens = DataGenUtils.getFieldGensFromSerdes(fieldSerdes, rnd, false);
                // Generate a tuple with random field values.
                Object[] fields = new Object[numFields];
                for (int j = 0; j < numFields; j++) {
                    fields[j] = fieldGens[j].next();
                }            
                // Create and write tuple to bytes using an LSMBTreeTupleWriter.
                ITupleReference tuple = TupleUtils.createTuple(fieldSerdes, (Object[])fields);
                LSMBTreeTupleWriter matterTupleWriter = new LSMBTreeTupleWriter(typeTraits, numKeyFields, false);
                LSMBTreeTupleWriter antimatterTupleWriter = new LSMBTreeTupleWriter(typeTraits, numKeyFields, true);
                ByteBuffer matterBuf = writeTuple(tuple, matterTupleWriter);
                ByteBuffer antimatterBuf = writeTuple(tuple, antimatterTupleWriter);

                // The antimatter buf should only contain keys, sanity check the size.
                if (numFields != numKeyFields) {
                    assertTrue(antimatterBuf.array().length < matterBuf.array().length);
                }

                // Tuple reference should work for both matter and antimatter tuples (doesn't matter which factory creates it).
                LSMBTreeTupleReference lsmBTreeTuple = (LSMBTreeTupleReference) matterTupleWriter.createTupleReference();
                
                // Use LSMBTree tuple reference to interpret the written tuples.
                // Repeat the block inside to test that repeated resetting to matter/antimatter tuples works.
                for (int r = 0; r < 4; r++) {
                    
                    // Check matter tuple with lsmBTreeTuple.
                    lsmBTreeTuple.resetByTupleOffset(matterBuf, 0);
                    assertEquals(numFields, lsmBTreeTuple.getFieldCount());
                    assertFalse(lsmBTreeTuple.isAntimatter());
                    Object[] deserMatterTuple = TupleUtils.deserializeTuple(lsmBTreeTuple, fieldSerdes);
                    for (int j = 0; j < numFields; j++) {
                        assertEquals(fields[j], deserMatterTuple[j]);
                    }
                    
                    // Check antimatter tuple with lsmBTreeTuple.
                    lsmBTreeTuple.resetByTupleOffset(antimatterBuf, 0);
                    // Should only contain keys.
                    assertEquals(numKeyFields, lsmBTreeTuple.getFieldCount());
                    assertTrue(lsmBTreeTuple.isAntimatter());
                    Object[] deserAntimatterTuple = TupleUtils.deserializeTuple(lsmBTreeTuple, fieldSerdes);
                    for (int j = 0; j < numKeyFields; j++) {
                        assertEquals(fields[j], deserAntimatterTuple[j]);
                    }
                    
                    // Check matter tuple with maxLsmBTreeTuple.
                    // We should be able to manually set a prefix of the fields 
                    // (the passed type traits in the tuple factory's constructor).
                    maxLsmBTreeTuple.setFieldCount(numFields);
                    maxLsmBTreeTuple.resetByTupleOffset(matterBuf, 0);
                    assertEquals(numFields, maxLsmBTreeTuple.getFieldCount());
                    assertFalse(maxLsmBTreeTuple.isAntimatter());
                    Object[] maxDeserMatterTuple = TupleUtils.deserializeTuple(maxLsmBTreeTuple, fieldSerdes);
                    for (int j = 0; j < numFields; j++) {
                        assertEquals(fields[j], maxDeserMatterTuple[j]);
                    }
                    
                    // Check antimatter tuple with maxLsmBTreeTuple.
                    maxLsmBTreeTuple.resetByTupleOffset(antimatterBuf, 0);
                    // Should only contain keys (hardcoded as 1 in the factory at beginning of this method).
                    assertEquals(numKeyFields, maxLsmBTreeTuple.getFieldCount());
                    assertTrue(maxLsmBTreeTuple.isAntimatter());
                    Object[] maxDeserAntimatterTuple = TupleUtils.deserializeTuple(maxLsmBTreeTuple, fieldSerdes);
                    for (int j = 0; j < numKeyFields; j++) {
                        assertEquals(fields[j], maxDeserAntimatterTuple[j]);
                    }
                    
                    // Resetting maxLsmBTreeTuple should set its field count to
                    // maxFieldSerdes.length, based on the its type traits.
                    maxLsmBTreeTuple.resetByTupleOffset(maxMatterBuf, 0);
                    assertEquals(maxFieldSerdes.length, maxLsmBTreeTuple.getFieldCount());
                    assertFalse(maxLsmBTreeTuple.isAntimatter());
                    Object[] maxMaxMatterTuple = TupleUtils.deserializeTuple(maxLsmBTreeTuple, maxFieldSerdes);
                    for (int j = 0; j < maxFieldSerdes.length; j++) {
                        assertEquals(maxFields[j], maxMaxMatterTuple[j]);
                    }
                }
            }
        }
    }
    
    @Test
    public void testLSMBTreeTuple() throws HyracksDataException {        
        ISerializerDeserializer[] intFields = new IntegerSerializerDeserializer[] {
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        testLSMBTreeTuple(intFields);
        
        ISerializerDeserializer[] stringFields = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE };
        testLSMBTreeTuple(stringFields);
        
        ISerializerDeserializer[] mixedFields = new ISerializerDeserializer[] {
                UTF8StringSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE,
                UTF8StringSerializerDeserializer.INSTANCE, UTF8StringSerializerDeserializer.INSTANCE,
                IntegerSerializerDeserializer.INSTANCE };
        testLSMBTreeTuple(mixedFields);
    }
}