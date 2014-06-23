/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.nephele.managementgraph;

import eu.stratosphere.nephele.AbstractID;

/**
 * A management vertex ID uniquely identifies a {@link ManagementVertex}.
 * <p>
 * This class is not thread-safe.
 * 
 */
public final class ManagementVertexID extends AbstractID {
	
	private static final long serialVersionUID = 1L;
	
	/**
	 * Creates a new random management id.
	 */
	public ManagementVertexID() {
		super();
	}
	
	/**
	 * Creates a new management id, equal to the given id.
	 * 
	 * @param from The id to copy.
	 */
	public ManagementVertexID(AbstractID from) {
		super(from);
	}
}
