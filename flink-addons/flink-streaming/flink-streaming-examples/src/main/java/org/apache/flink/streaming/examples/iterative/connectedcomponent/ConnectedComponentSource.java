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

package org.apache.flink.streaming.examples.iterative.connectedcomponent;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.function.source.SourceFunction;
import org.apache.flink.util.Collector;

public class ConnectedComponentSource implements SourceFunction<Tuple2<Integer, Integer>> {
	private static final long serialVersionUID = 1L;
	
	private int[][] dataset;
	private Tuple2<Integer, Integer> outRecord = new Tuple2<Integer, Integer>();
	public ConnectedComponentSource() {
		dataset = new int[][] { { 4, 2 }, { 2, 1 }, { 2, 3 }, { 1, 3 },
				{ 7, 8 }, { 8, 9 }, { 9, 7 }, { 5, 6 } };
	}
	
	@Override
	public void invoke(Collector<Tuple2<Integer, Integer>> collector) throws Exception {
		for (int[] data : dataset){
			outRecord.f0=data[0];
			outRecord.f1=data[1];
			collector.collect(outRecord);
		}
	}

}
