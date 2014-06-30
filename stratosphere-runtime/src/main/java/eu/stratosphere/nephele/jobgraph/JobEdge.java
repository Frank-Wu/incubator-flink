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

/**
 * This class represent edges (communication channels) in a job graph.
 * The edges always go from an intermediate result partition to a job vertex.
 * An edge is parameterized with its {@link DistributionPattern}.
 */
public class JobEdge implements java.io.Serializable {

	private static final long serialVersionUID = 1L;
	
	
	/** The data set at the source of the edge. */
	private final IntermediateDataSet source;
	
	/** The vertex connected to this edge. */
	private final AbstractJobVertex target;

	/** The distribution pattern that should be used for this job edge. */
	private final DistributionPattern distributionPattern;

	/**
	 * Constructs a new job edge.
	 * 
	 * @param source The data set that is at the source of this edge.
	 * @param target The operation that is at the target of this edge.
	 * @param distributionPattern The pattern that defines how the connection behaves in parallel.
	 */
	public JobEdge(IntermediateDataSet source, AbstractJobVertex target, DistributionPattern distributionPattern) {
		this.source = source;
		this.target = target;
		this.distributionPattern = distributionPattern;
	}


	/**
	 * Returns the data set at the source of the edge.
	 * 
	 * @return The data set at the source of the edge
	 */
	public IntermediateDataSet getSource() {
		return source;
	}

	/**
	 * Returns the vertex connected to this edge.
	 * 
	 * @return The vertex connected to this edge.
	 */
	public AbstractJobVertex getTarget() {
		return target;
	}
	
	/**
	 * Returns the distribution pattern used for this edge.
	 * 
	 * @return The distribution pattern used for this edge.
	 */
	public DistributionPattern getDistributionPattern(){
		return this.distributionPattern;
	}
}
