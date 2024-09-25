/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.sysds.runtime.transform.encode;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DependencyTask;
import org.apache.sysds.runtime.util.DependencyThreadPool;

import java.util.*;
import java.util.concurrent.Callable;

public class ColumnEncoderBagOfWords extends ColumnEncoder {

	private HashMap<String, Integer> wordDictionary;
	public String regex = "\\s+"; // whitespace

	protected ColumnEncoderBagOfWords(int colID) {
		super(colID);
		wordDictionary = new HashMap<>();
	}

	@Override
	public int getDomainSize(){
		System.out.println("Alloc Done");
		return wordDictionary.size();
	}

	@Override
	protected double getCode(CacheBlock<?> in, int row) {
		throw new NotImplementedException();
	}

	@Override
	protected double[] getCodeCol(CacheBlock<?> in, int startInd, int rowEnd, double[] tmp) {
		throw new NotImplementedException();
	}

	@Override
	protected TransformType getTransformType() {
		return null;
	}

	public List<DependencyTask<?>> getBuildTasks(CacheBlock<?> in){
		List<Callable<Object>> tasks = new ArrayList<>();
		tasks.add(getBuildTask(in));
		return DependencyThreadPool.createDependencyTasks(tasks, null);
	}



	public Callable<Object> getBuildTask(CacheBlock<?> in) {
		return new ColumnBagOfWordsBuildTask(this, in);
	}

	@Override
	public void build(CacheBlock<?> in) {
		int i = 0;
		for (int r = 0; r < in.getNumRows(); r++) {
			String current = in.getString(r, this._colID - 1);
			if(current != null)
				for(String word : current.split(regex))
					if(!word.isEmpty())
						if(!this.wordDictionary.containsKey(word))
							this.wordDictionary.put(word, i++);
		}
		System.out.println("Built Done");
	}

	@Override
	protected void applyDense(CacheBlock<?> in, MatrixBlock out, int outputCol, int rowStart, int blk){
		for (int r = rowStart; r < Math.max(in.getNumRows(), rowStart + blk); r++) {
			String current = in.getString(r, this._colID);
			HashMap<String, Integer> counter = new HashMap<>();
			for (String word : current.split(regex))
				if (!word.isEmpty()) {
					Integer old = counter.getOrDefault(word, 0);
					counter.put(word, old + 1);
				}
			for (String word : counter.keySet()) {
				int c = this._colID + wordDictionary.get(word);
				out.set(r, c, counter.get(word));
			}
		}
		System.out.println("Apply Done");
	}

	@Override
	public void allocateMetaData(FrameBlock meta) {

	}

	@Override
	public FrameBlock getMetaData(FrameBlock out) {
		return out;
	}

	@Override
	public void initMetaData(FrameBlock meta) {

	}

	private static class ColumnBagOfWordsBuildTask implements Callable<Object> {

		private final ColumnEncoderBagOfWords _encoder;
		private final CacheBlock<?> _input;

		protected ColumnBagOfWordsBuildTask(ColumnEncoderBagOfWords encoder, CacheBlock<?> input) {
			_encoder = encoder;
			_input = input;
		}

		@Override
		public Void call() throws Exception {
			System.out.println("HEY");
			_encoder.build(_input);
			return null;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "<ColId: " + _encoder._colID + ">";
		}

	}
}
