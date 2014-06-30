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

package eu.stratosphere.core.io;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


/**
 * An input split referring to input data which is located on one or more hosts.
 */
public class LocatableInputSplit implements InputSplit {

	/** The number of the split. */
	private int splitNumber;

	/** The names of the hosts storing the data this input split refers to. */
	private String[] hostnames;

	/**
	 * Creates a new input split.
	 * 
	 * @param splitNumber
	 *        the number of the split
	 * @param hostnames
	 *        the names of the hosts storing the data this input split refers to
	 */
	public LocatableInputSplit(int splitNumber, String[] hostnames) {
		this.splitNumber = splitNumber;
		this.hostnames = hostnames == null ? new String[0] : hostnames;
	}

	/**
	 * Default constructor for serialization/deserialization.
	 */
	public LocatableInputSplit() {}

	/**
	 * Returns the names of the hosts storing the data this input split refers to
	 * 
	 * @return the names of the hosts storing the data this input split refers to
	 */
	public String[] getHostnames() {
		return this.hostnames;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(this.splitNumber);
		out.writeInt(this.hostnames.length);
		for (int i = 0; i < this.hostnames.length; i++) {
			StringRecord.writeString(out, this.hostnames[i]);
		}
	}

	@Override
	public void read(DataInput in) throws IOException {
		this.splitNumber = in.readInt();

		final int numHosts = in.readInt();
		this.hostnames = new String[numHosts];
		for (int i = 0; i < numHosts; i++) {
			this.hostnames[i] = StringRecord.readString(in);
		}
	}

	@Override
	public int getSplitNumber() {
		return this.splitNumber;
	}
}
