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

package eu.stratosphere.streaming.test.window.wordcount;

import java.net.InetSocketAddress;

import org.apache.log4j.Level;

import eu.stratosphere.client.minicluster.NepheleMiniCluster;
import eu.stratosphere.client.program.Client;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.nephele.jobgraph.JobGraph;
import eu.stratosphere.streaming.api.JobGraphBuilder;
import eu.stratosphere.streaming.util.LogUtils;
import eu.stratosphere.types.StringValue;

//TODO: window operator remains unfinished.
public class WindowWordCountLocal {

	public static JobGraph getJobGraph() {
		JobGraphBuilder graphBuilder = new JobGraphBuilder("testGraph");
		graphBuilder.setSource("WindowWordCountSource",
				WindowWordCountSource.class);
		graphBuilder.setTask("WindowWordCountSplitter",
				WindowWordCountSplitter.class, 1);
		graphBuilder.setTask("WindowWordCountCounter",
				WindowWordCountCounter.class, 1);
		graphBuilder.setSink("WindowWordCountSink", WindowWordCountSink.class);

		graphBuilder.broadcastConnect("WindowWordCountSource",
				"WindowWordCountSplitter");
		graphBuilder.fieldsConnect("WindowWordCountSplitter",
				"WindowWordCountCounter", 0, StringValue.class);
		graphBuilder.broadcastConnect("WindowWordCountCounter",
				"WindowWordCountSink");

		return graphBuilder.getJobGraph();
	}

	public static void main(String[] args) {

		LogUtils.initializeDefaultConsoleLogger(Level.DEBUG, Level.INFO);

		try {

			JobGraph jG = getJobGraph();
			Configuration configuration = jG.getJobConfiguration();

			if (args.length == 0) {
				args = new String[] { "local" };
			}

			if (args[0].equals("local")) {
				System.out.println("Running in Local mode");
				NepheleMiniCluster exec = new NepheleMiniCluster();

				exec.start();

				Client client = new Client(new InetSocketAddress("localhost",
						6498), configuration);

				client.run(jG, true);

				exec.stop();

			} else if (args[0].equals("cluster")) {
				System.out.println("Running in Cluster2 mode");

				Client client = new Client(new InetSocketAddress(
						"hadoop02.ilab.sztaki.hu", 6123), configuration);

				client.run(jG, true);

			}

		} catch (Exception e) {
			System.out.println(e);
		}

	}
}
