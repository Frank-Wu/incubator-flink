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

package eu.stratosphere.streaming.examples.batch.wordcount;

import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.streaming.api.invokable.UserTaskInvokable;
import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

public class BatchWordCountSplitter extends UserTaskInvokable {

	

	private String[] words = new String[] {};
	private StreamRecord outputRecord = new StreamRecord(new Tuple2<String,Long>());

	private Long timestamp =0L;
	

	@Override
	public void invoke(StreamRecord record) throws Exception {
		int numberOfRecords = record.getNumOfTuples();
		for (int i = 0; i < numberOfRecords; ++i) {
			words = record.getString(0).split(" ");
			timestamp=record.getLong(1);
			for (String word : words) {
				outputRecord.setString(0, word);
				outputRecord.setLong(1, timestamp);
				emit(outputRecord);
			}
		}
	}
}