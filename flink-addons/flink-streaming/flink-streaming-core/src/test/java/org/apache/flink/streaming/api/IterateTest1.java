/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.flink.streaming.api;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.junit.Test;

public class IterateTest1 {

	private static final long MEMORYSIZE = 32;

	class ValueProgress{
		public int value;
		public Long progress;
	}
	
	class IterationSource implements SourceFunction<Tuple2<String, Object>>{

		private static final long serialVersionUID = 1L;

		private Tuple2<String, Object> outTuple=new Tuple2<String, Object>();
		private ValueProgress vp = new ValueProgress();
		int value = 0;
		Long progress = 0L;
		
		@Override
		public void invoke(Collector<Tuple2<String, Object>> collector)
				throws Exception {
			outTuple.f0="source";
			while (true){
				vp.value=value;
				vp.progress=progress;
				outTuple.f1 = vp;
				collector.collect(outTuple);
				value += 2;
				progress += 1;
			}
		}
	}
	
	class IterationFixpointer extends RichFlatMapFunction<Tuple2<String, Object>, Tuple2<String, Object>> {

		private static final long serialVersionUID = 1L;
		private int iteration=0;
		private Tuple2<String, Object> outTuple=new Tuple2<String, Object>();
		
		@Override
		public void flatMap(Tuple2<String, Object> value, Collector<Tuple2<String, Object>> out) throws Exception {
			if(iteration==1000){
				outTuple.f0="sink";
				out.collect(outTuple);
			}else{
				outTuple.f1="updater";
				out.collect(outTuple);
			}
		}

	}
	
	class IterationSelector extends OutputSelector<Tuple2<String, Object>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void select(Tuple2<String, Object> value, Collection<String> outputs) {
			if (value.f0=="sink") {
				outputs.add("sink");
			} else {
				outputs.add("updater");
			}			
		}

	}
	
	class IterationUpdater extends RichFlatMapFunction<Object, Tuple2<String, Object>> {

		private static final long serialVersionUID = 1L;

		private Tuple2<String, Object> outTuple=new Tuple2<String, Object>();
		@Override
		public void flatMap(Object value, Collector<Tuple2<String, Object>> out)
				throws Exception {
			//do some updates
			outTuple.f0="updater";
			out.collect(outTuple);
		}

	}


	@Test
	public void test() throws Exception {
		LogUtils.initializeDefaultConsoleLogger(Level.OFF, Level.OFF);

		LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);
		
		List<Integer> bl = new ArrayList<Integer>();
		for (int i = 0; i < 10000; i++) {
			bl.add(i);
		}
		DataStream<Integer> source = env.fromCollection(bl);

		IterativeDataStream<Integer> iteration = source.iterate();

		DataStream<Integer> itHead = iteration.map(new IterationHead()).flatMap(new ItSelect());

		iteration.closeWith(itHead).print().name("print");

		env.executeTest(MEMORYSIZE);
	}

}
