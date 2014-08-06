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
import java.util.List;

import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeDataStream;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.sink.SinkFunction;
import org.apache.flink.streaming.util.LogUtils;
import org.apache.flink.util.Collector;
import org.apache.log4j.Level;
import org.junit.Test;

public class IterateTest2 {

	private static final long MEMORYSIZE = 32;

	public static final class IterationHead extends RichFlatMapFunction<Integer, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Integer value, Collector<Integer> out) throws Exception {
			System.out.println("iteration head ="+value);
			out.collect(value);
		}

	}

	public static final class IterationTail extends RichFlatMapFunction<Integer, Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Integer value, Collector<Integer> out) throws Exception {
			System.out.println("iteration tail ="+value);
			out.collect(value);
		}

	}

	public static final class MySink implements SinkFunction<Integer> {

		private static final long serialVersionUID = 1L;

		@Override
		public void invoke(Integer tuple) {
			System.out.println("sink value="+tuple);
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

		DataStream<Integer> increment = iteration.flatMap(new IterationHead()).flatMap(
				new IterationTail());

		iteration.closeWith(increment).addSink(new MySink());

		env.executeTest(MEMORYSIZE);

	}

}
