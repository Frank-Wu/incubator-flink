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

package eu.stratosphere.nephele.jobgraph;

import eu.stratosphere.api.common.io.InitializeOnMaster;
import eu.stratosphere.api.common.io.OutputFormat;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

/**
 * A task vertex that run an initialization on the master, trying to deserialize an output format
 * and initializing it on master, if necessary.
 */
public class OutputFormatVertex extends AbstractJobVertex {
	
	private static final long serialVersionUID = 1L;
	
	
	/** Caches the output format associated to this output vertex. */
	private transient OutputFormat<?> outputFormat;

	/**
	 * Creates a new task vertex with the specified name.
	 * 
	 * @param name The name of the task vertex.
	 */
	public OutputFormatVertex(String name) {
		super(name);
	}
	
	/**
	 * Creates a new task vertex with the specified name.
	 * 
	 * @param name The name of the task vertex.
	 * @param outputFormat The output format for the vertex.
	 */
	public OutputFormatVertex(String name, OutputFormat<?> outputFormat) {
		super(name);
		this.outputFormat = outputFormat;
	}
	
	
	@Override
	public void initializeOnMaster(ClassLoader loader) throws Exception {
		if (this.outputFormat == null) {
			TaskConfig cfg = new TaskConfig(getConfiguration());
			UserCodeWrapper<OutputFormat<?>> wrapper = cfg.<OutputFormat<?>>getStubWrapper(loader);
		
			if (wrapper == null) {
				throw new Exception("No output format present in OutputFormatVertex's task configuration.");
			}

			this.outputFormat = wrapper.getUserCodeObject(OutputFormat.class, loader);
			this.outputFormat.configure(cfg.getStubParameters());
		}
		
		if (this.outputFormat instanceof InitializeOnMaster) {
			((InitializeOnMaster) this.outputFormat).initializeGlobal(getParallelism());
		}
	}
}
