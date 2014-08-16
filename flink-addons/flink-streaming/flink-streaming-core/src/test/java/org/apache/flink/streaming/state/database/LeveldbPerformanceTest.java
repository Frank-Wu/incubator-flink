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

package org.apache.flink.streaming.state.database;

import java.util.Random;

import org.junit.Test;

public class LeveldbPerformanceTest {

	static Random rand=new Random();
	static final String AB="0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	static final int stringLength=20;
	static final int numTuple=10000;
	static final int maxInt=100000;
	
	public static String generateRandomString(int stringLength){
		StringBuilder sb=new StringBuilder();
		for(int i=0; i<stringLength; ++i){
			sb.append(AB.charAt(rand.nextInt(AB.length())));
		}
		return sb.toString();
	}
	
	@Test
	public void performanceTest(){
		LeveldbState state=new LeveldbState("test");
		long startTime, endTime;
		double elapsedSeconds;
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < numTuple; ++i) {
			String key = generateRandomString(stringLength);
			String value = generateRandomString(stringLength);
			state.setTuple(key, value);
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
		state.close();
	}
}
