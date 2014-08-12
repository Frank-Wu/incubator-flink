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

import org.junit.Test;

public class DatabaseTest {
	
	@Test
	public void LeveldbTest(){
		LeveldbState state=new LeveldbState("test");
		state.setTuple("hello", "world");
		System.out.println(state.getTuple("hello"));
		state.setTuple("big", "data");
		state.setTuple("flink", "streaming");
		LeveldbStateIterator iterator=state.getIterator();
		while(iterator.hasNext()){
			String key=iterator.getNextKey();
			String value=iterator.getNextValue();
			System.out.println("key="+key+", value="+value);
			iterator.next();
		}
		state.close();
	}
	
	//please remind starting Redis service before testing.
	@Test
	public void RedisTest(){
		RedisState state=new RedisState();
		state.setTuple("hello", "world");
		System.out.println(state.getTuple("hello"));
		state.setTuple("big", "data");
		state.setTuple("flink", "streaming");
		RedisStateIterator iterator=state.getIterator();
		while(iterator.hasNext()){
			String key=iterator.getNextKey();
			String value=iterator.getNextValue();
			System.out.println("key="+key+", value="+value);
			iterator.next();
		}
		state.close();
	}
	
	//please remind starting mysql service before testing.
	@Test
	public void MysqlTest(){
		MysqlState state=new MysqlState("testdb", "root", "yingjun");
		String sql = "create table flinkdb(mykey varchar(20), myvalue varchar(20))";
		state.executeUpdate(sql);
		sql = "insert into flinkdb values('hello', 'world')";
		state.executeUpdate(sql);
		sql = "insert into flinkdb values('big', 'data')";
		state.executeUpdate(sql);
		sql = "insert into flinkdb values('flink', 'streaming')";
		state.executeUpdate(sql);
		sql = "select * from flinkdb";
		ResultSet results = state.executeQuery(sql);
		try {
			while(results.next()){
				String key=results.getString("mykey");
				String value=results.getString("myvalue");
				System.out.println("mykey="+key+", myvalue="+value);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		state.close();
	}
	
	@Test
	public void MemcachedState(){
		MemcachedState state=new MemcachedState();
		state.setTuple("hello", "world");
		state.setTuple("big", "data");
		state.setTuple("flink", "streaming");
		System.out.println(state.getTuple("hello"));
		System.out.println(state.getTuple("big"));
		System.out.println(state.getTuple("flink"));
		state.close();
	}
}
