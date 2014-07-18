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

import java.util.ArrayList;

import org.apache.flink.streaming.state.SlidingWindow;
import org.apache.flink.streaming.state.TableState;
import org.apache.flink.streaming.state.TableStateIterator;
import org.junit.Test;
import org.apache.flink.api.java.tuple.Tuple2;

public class InternalStateTest {

	@Test
	public void MutableTableStateTest() {
		TableState<String, String> state = new TableState<String, String>();
		state.put("abc", "hello");
		state.put("test", "world");
		state.put("state", "mutable");
		state.put("streaming", "persist");
		String s = state.get("streaming");
		if (s == null) {
			System.out.println("key does not exist!");
		} else {
			System.out.println("value=" + s);
		}
		s = state.get("null");
		if (s == null) {
			System.out.println("key does not exist!");
		} else {
			System.out.println("value=" + s);
		}
		TableStateIterator<String, String> iterator = state.getIterator();
		while (iterator.hasNext()) {
			Tuple2<String, String> tuple = iterator.next();
			System.out.println(tuple.getField(0) + ", " + tuple.getField(1));
		}
	}

	@Test
	public void WindowStateTest() {
		SlidingWindow state = new SlidingWindow(10, 2, 1);
		for (int i = 0; i < 5; ++i) {
			Tuple2<String, Integer> tuple1 = new Tuple2<String, Integer>();
			tuple1.f0 = "hello";
			tuple1.f1 = 100 + i;
			Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>();
			tuple2.f0 = "world";
			tuple2.f1 = 200 + i;
			ArrayList<Tuple2<String, Integer>> tupleArray = new ArrayList<Tuple2<String, Integer>>();
			tupleArray.add(tuple1);
			tupleArray.add(tuple2);
			state.pushBack(tupleArray);
		}
		SlidingWindowIterator<Tuple2<String, Integer>> iterator = 
				new SlidingWindowIterator<Tuple2<String, Integer>>(state);
		while(iterator.hasNext()){
			Tuple2<String, Integer> tuple = iterator.next();
			System.out.println(tuple.f0+", "+tuple.f1);
		}
	}
}
