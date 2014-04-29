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

package eu.stratosphere.streaming.state;

import org.apache.commons.collections.buffer.CircularFifoBuffer;

import eu.stratosphere.streaming.api.streamrecord.StreamRecord;

/**
 * The window state for window operator. To be general enough, this class
 * implements a count based window operator. It is possible for the user to
 * compose time based window operator by extending this class by splitting the
 * stream into multiple mini batches.
 */
public class WindowInternalState<K> implements InternalState<K, StreamRecord> {
	private int currentRecordNum;
	private int fullRecordNum;
	private int slideRecordNum;
	CircularFifoBuffer buffer;

	public WindowInternalState(int windowSize, int slidingStep) {
		currentRecordNum = 0;
		fullRecordNum = windowSize;
		slideRecordNum = slidingStep;
		buffer = new CircularFifoBuffer(windowSize);
	}

	public void pushBack(StreamRecord records) {
		buffer.add(records);
		currentRecordNum += 1;
	}

	public StreamRecord popFront() {
		StreamRecord frontRecord=(StreamRecord) buffer.get();
		buffer.remove();
		return frontRecord;
	}

	public boolean isFull() {
		return currentRecordNum >= fullRecordNum;
	}

	public boolean isComputable() {
		if (currentRecordNum == fullRecordNum + slideRecordNum) {
			currentRecordNum -= slideRecordNum;
			return true;
		}
		return false;
	}

	@Override
	public void put(K key, StreamRecord value) {
		// TODO Auto-generated method stub

	}

	@Override
	public StreamRecord get(K key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void delete(K key) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean containsKey(K key) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public StateIterator<K, StreamRecord> getIterator() {
		// TODO Auto-generated method stub
		return null;
	}

}
