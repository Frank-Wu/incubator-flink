/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.api.common.io;

import eu.stratosphere.core.io.InputSplit;


public interface InputSplitSource<T extends InputSplit> extends java.io.Serializable {
	
	/**
	 * Returns the input split type of the input splits.
	 *
	 * @return The input split type class.
	 */
	Class<? extends T> getInputSplitType();

	/**
	 * Computes the input splits. The given minimum number of splits is a hint as to how
	 * many splits are desired.
	 *
	 * @param minNumSplits Number of minimal input splits, as a hint.
	 * @return An array of input splits.
	 * 
	 * @throws Exception Exceptions when creating the input splits may be forwarded and will cause the
	 *                   execution to permanently fail.
	 */
	T[] createInputSplits(int minNumSplits) throws Exception;
}
