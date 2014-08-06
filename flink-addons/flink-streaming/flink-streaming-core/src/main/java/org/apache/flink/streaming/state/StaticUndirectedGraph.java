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

package org.apache.flink.streaming.state;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class StaticUndirectedGraph {
	public Map<Integer, Set<Integer>> _vertices = null;

	public StaticUndirectedGraph() {
		_vertices = new HashMap<Integer, Set<Integer>>();
	}

	public void insertEdge(int sourceNode, int targetNode){
		if(!_vertices.containsKey(sourceNode)){
			_vertices.put(sourceNode, new HashSet<Integer>());
		}
		if(!_vertices.containsKey(targetNode)){
			_vertices.put(targetNode, new HashSet<Integer>());
		}
		_vertices.get(sourceNode).add(targetNode);
		_vertices.get(targetNode).add(sourceNode);
	}
}
