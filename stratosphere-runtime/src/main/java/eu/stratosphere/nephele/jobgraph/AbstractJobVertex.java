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

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.Validate;

import eu.stratosphere.api.common.io.InputSplitSource;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.executiongraph.ExecutionVertex;
import eu.stratosphere.nephele.template.AbstractInvokable;

/**
 * An abstract base class for a job vertex.
 */
public class AbstractJobVertex implements java.io.Serializable {

	private static final long serialVersionUID = 1L;

	private static final String DEFAULT_NAME = "(unnamed vertex)";
	
	
	// --------------------------------------------------------------------------------------------
	// Members that define the structure / topology of the graph
	// --------------------------------------------------------------------------------------------
	
	/** List of produced data sets, one per writer */
	private final ArrayList<IntermediateDataSet> results = new ArrayList<IntermediateDataSet>();

	/** List of edges with incoming data. One per Reader. */
	private final ArrayList<JobEdge> inputs = new ArrayList<JobEdge>();

	/** The name of the vertex */
	private final String name;

	/** The ID of the vertex. */
	private final JobVertexID id;

	/** Number of subtasks to split this task into at runtime.*/
	private int parallelism = -1;

	/** Custom configuration passed to the assigned task at runtime. */
	private Configuration configuration;

	/** The class of the invokable. */
	private Class<? extends AbstractInvokable> invokableClass;
	
	/** Optionally, a source of input splits */
	private InputSplitSource<?> inputSplitSource;

	// --------------------------------------------------------------------------------------------
	// Members that track the execution status (all transient)
	// --------------------------------------------------------------------------------------------
	
	private transient List<ExecutionVertex> executionVertices;
	
	
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Constructs a new job vertex and assigns it with the given name.
	 * 
	 * @param name The name of the new job vertex.
	 */
	public AbstractJobVertex(String name) {
		this(name, null);
	}
	
	/**
	 * Constructs a new job vertex and assigns it with the given name.
	 * 
	 * @param name The name of the new job vertex.
	 * @param id The id of the job vertex.
	 */
	public AbstractJobVertex(String name, JobVertexID id) {
		this.name = name == null ? DEFAULT_NAME : name;
		this.id = id == null ? new JobVertexID() : id;
	}
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the ID of this job vertex.
	 * 
	 * @return The ID of this job vertex
	 */
	public JobVertexID getID() {
		return this.id;
	}
	
	/**
	 * Returns the name of the vertex.
	 * 
	 * @return The name of the vertex.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Returns the number of produced intermediate data sets.
	 * 
	 * @return The number of produced intermediate data sets.
	 */
	public int getNumberOfProducedIntermediateDataSets() {
		return this.results.size();
	}

	/**
	 * Returns the number of inputs.
	 * 
	 * @return The number of inputs.
	 */
	public int getNumberOfInputs() {
		return this.inputs.size();
	}

	/**
	 * Returns the vertex's configuration object which can be used to pass custom settings to the task at runtime.
	 * 
	 * @return the vertex's configuration object
	 */
	public Configuration getConfiguration() {
		if (this.configuration == null) {
			this.configuration = new Configuration();
		}
		return this.configuration;
	}
	
	public void setInvokableClass(Class<? extends AbstractInvokable> invokable) {
		Validate.notNull(invokable);
		this.invokableClass = invokable;
	}
	
	/**
	 * Returns the invokable class which represents the task of this vertex
	 * 
	 * @return the invokable class, <code>null</code> if it is not set
	 */
	public Class<? extends AbstractInvokable> getInvokableClass() {
		return this.invokableClass;
	}
	
	/**
	 * Gets the degree of parallelism of the task.
	 * 
	 * @return The degree of parallelism of the task.
	 */
	public int getParallelism() {
		return parallelism;
	}

	/**
	 * Sets the degree of parallelism for the task.
	 * 
	 * @param parallelism The degree of parallelism for the task.
	 */
	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}
	
	public InputSplitSource<?> getInputSplitSource() {
		return inputSplitSource;
	}

	public void setInputSplitSource(InputSplitSource<?> inputSplitSource) {
		this.inputSplitSource = inputSplitSource;
	}
	
	public List<IntermediateDataSet> getProducedDataSets() {
		return this.results;
	}
	
	public List<JobEdge> getInputs() {
		return this.inputs;
	}
	
	// --------------------------------------------------------------------------------------------

	public IntermediateDataSet createAndAddResultDataSet() {
		return createAndAddResultDataSet(new IntermediateDataSetID());
	}
	
	public IntermediateDataSet createAndAddResultDataSet(IntermediateDataSetID id) {
		IntermediateDataSet result = new IntermediateDataSet(id, this);
		this.results.add(result);
		return result;
	}
	
	public void connectDataSetAsInput(IntermediateDataSet dataSet, DistributionPattern distPattern) {
		JobEdge edge = new JobEdge(dataSet, this, distPattern);
		this.inputs.add(edge);
		dataSet.addConsumer(edge);
	}
	
	public void connectNewDataSetAsInput(AbstractJobVertex input, DistributionPattern distPattern) {
		IntermediateDataSet dataSet = input.createAndAddResultDataSet();
		JobEdge edge = new JobEdge(dataSet, this, distPattern);
		this.inputs.add(edge);
		dataSet.addConsumer(edge);
	}
	
	// --------------------------------------------------------------------------------------------
	
	public boolean isInputVertex() {
		return this.inputs.isEmpty();
	}
	
	public boolean isOutputVertex() {
		return this.results.isEmpty();
	}
	
	public boolean hasNoConnectedInputs() {
		for (JobEdge edge : inputs) {
			if (edge.getSource().isConnected()) {
				return false;
			}
		}
		
		return true;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * A hook that can be overwritten by sub classes to implement logic that is called by the 
	 * master when the job starts.
	 * 
	 * @param loader The class loader for user defined code.
	 * @throws Exception The method may throw exceptions which cause the job to fail immediately.
	 */
	public void initializeOnMaster(ClassLoader loader) throws Exception {}
	
	// --------------------------------------------------------------------------------------------

	@Override
	public String toString() {
		return this.name + " (" + this.invokableClass + ')';
	}
}
