/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2014 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.streaming.examples.wordcount;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;
import eu.stratosphere.streaming.state.MutableInternalState;
import eu.stratosphere.streaming.util.PerformanceCounter;

public class WordCountKvCounter extends UserTaskInvokable {

	private MutableInternalState<String, Integer> wordCounts=new MutableInternalState<String, Integer>();
	private String word = "";
	private Integer count = 0;

	PerformanceCounter perf = new PerformanceCounter("CounterEmitCounter"+this.name, 1000, 1000);

	
	private StreamRecord outRecord = new StreamRecord(new Tuple2<String, Integer>());

	@Override
	public void invoke(StreamRecord record) throws Exception {
		word = record.getString(0);
		
		if (wordCounts.containsKey(word)) {
			count = wordCounts.get(word) + 1;
			wordCounts.put(word, count);
		} else {
			count=1;
			wordCounts.put(word, 1);
		}
		
		outRecord.setString(0,word);
		outRecord.setInteger(1,count);

		emit(outRecord);
		perf.count();
	}
}
