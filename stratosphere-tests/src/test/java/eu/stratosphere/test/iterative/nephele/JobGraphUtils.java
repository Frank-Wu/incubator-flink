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

package eu.stratosphere.test.iterative.nephele;

import java.io.IOException;

import eu.stratosphere.api.common.operators.util.UserCodeObjectWrapper;
import eu.stratosphere.api.common.operators.util.UserCodeWrapper;
import eu.stratosphere.api.common.io.FileInputFormat;
import eu.stratosphere.api.common.io.InputFormat;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.client.JobClient;
import eu.stratosphere.nephele.client.JobExecutionException;
import eu.stratosphere.nephele.jobgraph.DistributionPattern;
import eu.stratosphere.runtime.io.channels.ChannelType;
import eu.stratosphere.nephele.jobgraph.AbstractJobVertex;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.nephele.jobgraph.JobGraphDefinitionException;
import eu.stratosphere.nephele.jobgraph.InputFormatVertex;
import eu.stratosphere.nephele.jobgraph.OutputFormatVertex;
import eu.stratosphere.nephele.jobgraph.JobTaskVertex;
import eu.stratosphere.pact.runtime.iterative.io.FakeOutputTask;
import eu.stratosphere.pact.runtime.iterative.task.IterationSynchronizationSinkTask;
import eu.stratosphere.pact.runtime.task.DataSinkTask;
import eu.stratosphere.pact.runtime.task.DataSourceTask;
import eu.stratosphere.pact.runtime.task.RegularPactTask;
import eu.stratosphere.pact.runtime.task.util.TaskConfig;

public class JobGraphUtils {

	public static final long MEGABYTE = 1024l * 1024l;

	private JobGraphUtils() {
	}

	public static void submit(JobGraph graph, Configuration nepheleConfig) throws IOException, JobExecutionException {
		JobClient client = new JobClient(graph, nepheleConfig);
		client.submitJobAndWait();
	}
	
	public static <T extends FileInputFormat<?>> InputFormatVertex createInput(T stub, String path, String name, JobGraph graph,
			int degreeOfParallelism)
	{
		stub.setFilePath(path);
		return createInput(new UserCodeObjectWrapper<T>(stub), name, graph, degreeOfParallelism);
	}

	private static <T extends InputFormat<?,?>> InputFormatVertex createInput(UserCodeWrapper<T> stub, String name, JobGraph graph,
			int degreeOfParallelism)
	{
		InputFormatVertex inputVertex = new InputFormatVertex(name, graph);
		
		inputVertex.setInvokableClass(DataSourceTask.class);
		
		inputVertex.setNumberOfSubtasks(degreeOfParallelism);

		TaskConfig inputConfig = new TaskConfig(inputVertex.getConfiguration());
		inputConfig.setStubWrapper(stub);
		
		return inputVertex;
	}

//	public static void connect(AbstractJobVertex source, AbstractJobVertex target, ChannelType channelType,
//			DistributionPattern distributionPattern, ShipStrategyType shipStrategy) throws JobGraphDefinitionException
//	{
//		source.connectTo(target, channelType, CompressionLevel.NO_COMPRESSION, distributionPattern);
//		new TaskConfig(source.getConfiguration()).addOutputShipStrategy(shipStrategy);
//	}
	
	public static void connect(AbstractJobVertex source, AbstractJobVertex target, ChannelType channelType,
			DistributionPattern distributionPattern) throws JobGraphDefinitionException
	{
		source.connectTo(target, channelType, distributionPattern);
	}

	public static JobTaskVertex createTask(@SuppressWarnings("rawtypes") Class<? extends RegularPactTask> task, String name, JobGraph graph,
			int degreeOfParallelism)
	{
		JobTaskVertex taskVertex = new JobTaskVertex(name, graph);
		taskVertex.setInvokableClass(task);
		taskVertex.setNumberOfSubtasks(degreeOfParallelism);
		return taskVertex;
	}

	public static OutputFormatVertex createSync(JobGraph jobGraph, int degreeOfParallelism) {
		OutputFormatVertex sync = new OutputFormatVertex("BulkIterationSync", jobGraph);
		sync.setInvokableClass(IterationSynchronizationSinkTask.class);
		sync.setNumberOfSubtasks(1);
		TaskConfig syncConfig = new TaskConfig(sync.getConfiguration());
		syncConfig.setGateIterativeWithNumberOfEventsUntilInterrupt(0, degreeOfParallelism);
		return sync;
	}

	public static OutputFormatVertex createFakeOutput(JobGraph jobGraph, String name, int degreeOfParallelism)
	{
		OutputFormatVertex outputVertex = new OutputFormatVertex(name, jobGraph);
		outputVertex.setInvokableClass(FakeOutputTask.class);
		outputVertex.setNumberOfSubtasks(degreeOfParallelism);
		return outputVertex;
	}

	public static OutputFormatVertex createFileOutput(JobGraph jobGraph, String name, int degreeOfParallelism)
	{
		OutputFormatVertex sinkVertex = new OutputFormatVertex(name, jobGraph);
		sinkVertex.setInvokableClass(DataSinkTask.class);
		sinkVertex.setNumberOfSubtasks(degreeOfParallelism);
		return sinkVertex;
	}
}
