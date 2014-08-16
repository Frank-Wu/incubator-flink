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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.junit.Test;

public class MysqlPerformanceTest {

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
	
	//please remind starting mysql service before testing.
	@Test
	public void performanceTest(){
		MysqlState state=new MysqlState("testdb", "root", "password");
		long startTime, endTime;
		double elapsedSeconds;
		//if mykeyvalue table has already existed, then drop the table first.
		//state.executeUpdate("drop table mykeyvalue");
		state.executeUpdate("create table mykeyvalue(mykey varchar("+String.valueOf(stringLength)+"), myvalue int) engine=memory");
		//insert
		startTime=System.currentTimeMillis();
		for (int i = 0; i < numTuple; ++i) {
			String key = generateRandomString(stringLength);
			Integer value = rand.nextInt(maxInt);
			state.executeUpdate("insert into mykeyvalue values('" + key + "'," + value.toString() + ")");
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database insert elapsedTime="+elapsedSeconds+"s");
	
		//scan
		startTime=System.currentTimeMillis();
		try {
			ResultSet result = state.executeQuery("select avg(myvalue) as mysum from mykeyvalue");
			int mysum = 0;
			while (result.next()) {
				mysum = result.getInt("mysum");
				System.out.println("value=" + mysum);
			}
			result.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		endTime=System.currentTimeMillis();
		elapsedSeconds=(endTime-startTime)/1000.0;
		System.out.println("database scan elapsedTime="+elapsedSeconds+"s");
		
		state.close();
	}

}
