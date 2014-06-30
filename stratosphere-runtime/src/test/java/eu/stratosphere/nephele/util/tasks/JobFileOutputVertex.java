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

package eu.stratosphere.nephele.util.tasks;

import eu.stratosphere.core.fs.Path;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobVertexID;


public class JobFileOutputVertex extends AbstractJobVertex {

	private static final long serialVersionUID = 1L;

	public static final String PATH_PROPERTY = "outputPath";
	
	/**
	 * The path pointing to the output file/directory.
	 */
	private Path path;


	public JobFileOutputVertex(String name, JobVertexID id) {
		super(name, id);
	}
	
	public JobFileOutputVertex(String name) {
		super(name);
	}

	/**
	 * Sets the path of the file the job file input vertex's task should write to.
	 * 
	 * @param path
	 *        the path of the file the job file input vertex's task should write to
	 */
	public void setFilePath(Path path) {
		this.path = path;
		getConfiguration().setString(PATH_PROPERTY, path.toString());
	}

	/**
	 * Returns the path of the file the job file output vertex's task should write to.
	 * 
	 * @return the path of the file the job file output vertex's task should write to or <code>null</code> if no path
	 *         has yet been set
	 */
	public Path getFilePath() {
		return this.path;
	}
}