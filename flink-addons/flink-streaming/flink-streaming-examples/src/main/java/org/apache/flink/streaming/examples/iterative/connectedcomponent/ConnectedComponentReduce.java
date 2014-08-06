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

import java.util.LinkedList;

import org.apache.flink.api.java.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.state.StaticUndirectedGraph;
import org.apache.flink.streaming.state.VertexCompGraph;
import org.apache.flink.util.Collector;

public class ConnectedComponentReduce extends RichFlatMapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, LinkedList<Integer>>> {
	private static final long serialVersionUID = 1L;
	
	private StaticUndirectedGraph staticGraph = new StaticUndirectedGraph();
	private VertexCompGraph<Integer> dataGraph = new VertexCompGraph<Integer>();
	
	private Tuple2<Integer, Integer> outTuple = new Tuple2<Integer, Integer>();
	
	@Override
	public void flatMap(Tuple2<Integer, Integer> input, Collector<Tuple2<Integer, LinkedList<Integer>>> collector)
			throws Exception {
		int sourceNode=input.f0;
		int targetNode=input.f1;
		staticGraph.insertEdge(sourceNode, targetNode);
		dataGraph.setValue(sourceNode, sourceNode);
		dataGraph.setValue(targetNode, targetNode);
		collector.collect(outTuple);
	}

}
