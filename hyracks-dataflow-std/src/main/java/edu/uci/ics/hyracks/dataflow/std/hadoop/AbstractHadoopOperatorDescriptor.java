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
package edu.uci.ics.hyracks.dataflow.std.hadoop;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;

import edu.uci.ics.dcache.client.DCacheClient;
import edu.uci.ics.hyracks.api.dataflow.IDataWriter;
import edu.uci.ics.hyracks.api.dataflow.value.RecordDescriptor;
import edu.uci.ics.hyracks.api.job.JobSpecification;
import edu.uci.ics.hyracks.dataflow.std.base.AbstractSingleActivityOperatorDescriptor;
import edu.uci.ics.hyracks.dataflow.std.hadoop.util.DatatypeHelper;
import edu.uci.ics.hyracks.dataflow.std.hadoop.util.IHadoopClassFactory;

public abstract class AbstractHadoopOperatorDescriptor extends AbstractSingleActivityOperatorDescriptor {

	
	protected static class DataWritingOutputCollector<K, V> implements OutputCollector<K, V> {
        private IDataWriter<Object[]> writer;

        public DataWritingOutputCollector() {
        }

        public DataWritingOutputCollector(IDataWriter<Object[]> writer) {
            this.writer = writer;
        }

        @Override
        public void collect(Object key, Object value) throws IOException {
           	writer.writeData(new Object[] { key, value });
        }

        public void setWriter(IDataWriter<Object[]> writer) {
            this.writer = writer;
        }
    }

    public static String MAPRED_CACHE_FILES = "mapred.cache.files";
    public static String MAPRED_CACHE_LOCALFILES = "mapred.cache.localFiles";

    private static final long serialVersionUID = 1L;
    private final HashMap<String, String> jobConfMap;
    private IHadoopClassFactory hadoopClassFactory;
    
    public abstract RecordDescriptor getRecordDescriptor(JobConf jobConf);
    	
    public AbstractHadoopOperatorDescriptor(JobSpecification spec, RecordDescriptor recordDescriptor, JobConf jobConf,
            IHadoopClassFactory hadoopOperatorFactory) {
        super(spec, 1, 1);
        jobConfMap = DatatypeHelper.jobConf2HashMap(jobConf);
        this.hadoopClassFactory = hadoopOperatorFactory;
        if(recordDescriptor != null){
        	recordDescriptors[0]= recordDescriptor;
        }else{
        	recordDescriptors[0] = getRecordDescriptor(jobConf);
        }
    }

     
    public HashMap<String, String> getJobConfMap() {
		return jobConfMap;
	}


	public IHadoopClassFactory getHadoopClassFactory() {
		return hadoopClassFactory;
	}


	public void setHadoopClassFactory(IHadoopClassFactory hadoopClassFactory) {
		this.hadoopClassFactory = hadoopClassFactory;
	}


	protected Reporter createReporter() {
        return new Reporter() {
            @Override
            public Counter getCounter(Enum<?> name) {
                return null;
            }

            @Override
            public Counter getCounter(String group, String name) {
                return null;
            }    
              

            @Override
            public InputSplit getInputSplit() throws UnsupportedOperationException {
                return null;
            }

            @Override
            public void incrCounter(Enum<?> key, long amount) {

            }

            @Override
            public void incrCounter(String group, String counter, long amount) {

            }

            @Override
            public void progress() {

            }

            @Override
            public void setStatus(String status) {

            }
        };
    }

    public JobConf getJobConf() {
        return DatatypeHelper.hashMap2JobConf(jobConfMap);
    }
    
    public void populateCache(JobConf jobConf) {
        String cache = jobConf.get(MAPRED_CACHE_FILES);
        System.out.println("cache:" + cache);
        if (cache == null) {
            return;
        }
        String localCache = jobConf.get(MAPRED_CACHE_LOCALFILES);
        System.out.println("localCache:" + localCache);
        if (localCache != null) {
            return;
        }
        localCache = "";
        StringTokenizer cacheTokenizer = new StringTokenizer(cache, ",");
        while (cacheTokenizer.hasMoreTokens()) {
            if (!"".equals(localCache)) {
                localCache += ",";
            }
            try {
                localCache += DCacheClient.get().get(cacheTokenizer.nextToken());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        jobConf.set(MAPRED_CACHE_LOCALFILES, localCache);
        System.out.println("localCache:" + localCache);
    }
}