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
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.data.SparseBlockCSR;
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static org.apache.sysds.runtime.transform.encode.ColumnEncoderRecode.constructRecodeMapEntry;
import static org.apache.sysds.runtime.util.UtilFunctions.getEndIndex;

public class ColumnEncoderBagOfWords extends ColumnEncoder {

	public static int NUM_SAMPLES_MAP_ESTIMATION = 100;
	protected
	int[] nnzPerRow;
	private static final Pattern STOP_CHAR_PATTERN = Pattern.compile("[^a-zA-Z0-9\\s]");
	private HashMap<String, Integer> tokenDictionary;
	protected String seperatorRegex = "\\s+"; // whitespace
	protected boolean caseSensitive = false;
	protected long nnz = 0;

	protected ColumnEncoderBagOfWords(int colID) {
		super(colID);
		tokenDictionary = new HashMap<>();
	}

    public ColumnEncoderBagOfWords() {
        super(-1);
    }

	public void computeMapSizeEstimate(CacheBlock<?> in, int[] sampleIndices) {
		// Skip if estimate is already calculated
		if (getEstMetaSize() != 0)
			return;

		// Find the frequencies of distinct values in the sample after tokenization
		HashMap<String, Integer> distinctFreq = new HashMap<>();
		long totSize = 0;
		final int max_index = Math.min(ColumnEncoderBagOfWords.NUM_SAMPLES_MAP_ESTIMATION, sampleIndices.length);
		int totalNumWords = 0;
		for (int i = 0; i < max_index; i++) {
			int sind = sampleIndices[i];
			String current = in.getString(sind, this._colID - 1);
			if(current != null)
				for(String word : tokenize(current))
					if(!word.isEmpty()){
						if (distinctFreq.containsKey(word))
							distinctFreq.put(word, distinctFreq.get(word) + 1);
						else {
							distinctFreq.put(word, 1);
							// Maintain total size of the keys
							totSize += (word.length() * 2L + 16); //sizeof(String) = len(chars) + header
						}
						totalNumWords++;
					}
		}

		estimateDistinctTokens(sampleIndices.length, distinctFreq, totalNumWords, totSize);
	}

	@NotNull
	private String[] tokenize(String current) {
		return tokenize(current, this.caseSensitive, this.seperatorRegex);
	}

	@NotNull
	public static String[] tokenize(String current, boolean caseSensitive, String seperatorRegex) {
		// filter for characters that match: a-z, A-Z, 0-9
		current = current.replaceAll("'"," ");
		current = STOP_CHAR_PATTERN.matcher(caseSensitive ? current : current.toLowerCase())
				.replaceAll("");
		return current.split(seperatorRegex);
	}

	@Override
	public int getDomainSize(){
		return tokenDictionary.size();
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
		return TransformType.BAG_OF_WORDS;
	}



	public Callable<Object> getBuildTask(CacheBlock<?> in) {
		return new ColumnBagOfWordsBuildTask(this, in);
	}

	@Override
	public void build(CacheBlock<?> in) {
		int i = 0;
		this.nnz = 0;
		nnzPerRow = new int[in.getNumRows()];
		HashSet<String> current_dictionary;
		for (int r = 0; r < in.getNumRows(); r++) {
			current_dictionary = new HashSet<>();
			String current = in.getString(r, this._colID - 1);
			if(current != null)
				for(String word : tokenize(current))
					if(!word.isEmpty()){
						current_dictionary.add(word);
						if(!this.tokenDictionary.containsKey(word))
							this.tokenDictionary.put(word, i++);
					}
			this.nnzPerRow[r] = current_dictionary.size();
			this.nnz += current_dictionary.size();
		}
		// Overflow
		if(i < 0)
			throw new DMLRuntimeException("Token-Dictionary size of the BagOfWords Encoder on Col [" + this._colID +
					"] exceeds the maximum number of output columns");
	}

	// Pair class to hold key-value pairs (colId-tokenCount pairs)
	static class Pair {
		int key;
		int value;

		Pair(int key, int value) {
			this.key = key;
			this.value = value;
		}
	}

	protected void applySparse(CacheBlock<?> in, MatrixBlock out, int outputCol, int rowStart, int blk) {
		boolean mcsr = MatrixBlock.DEFAULT_SPARSEBLOCK == SparseBlock.Type.MCSR;
		mcsr = false; // force CSR for transformencode
		ArrayList<Integer> sparseRowsWZeros = new ArrayList<>();
		for(int r = rowStart; r < getEndIndex(in.getNumRows(), rowStart, blk); r++) {
			if(mcsr) {
				throw new NotImplementedException();
			}
			else { // csr
				HashMap<String, Integer> counter = countTokenAppearances(in, r);
				if(counter.size() > 0){
					SparseBlockCSR csrblock = (SparseBlockCSR) out.getSparseBlock();
					int rptr[] = csrblock.rowPointers();
					// assert that nnz from build is equal to nnz from apply
					assert counter.size() == nnzPerRow[r];
					Pair[] columnValuePairs = new Pair[counter.size()];
					int i = 0;
					for (Map.Entry<String, Integer> entry : counter.entrySet()) {
						String word = entry.getKey();
						columnValuePairs[i] = new Pair(outputCol + tokenDictionary.get(word), entry.getValue());
						i++;
					}

					// Sort the pairs array based on the columnId
					Arrays.sort(columnValuePairs, Comparator.comparingInt(pair -> pair.key));
					// Manually fill the column-indexes and values array
					for (i = 0; i < columnValuePairs.length; i++) {
						int index = sparseRowPointerOffset != null ? sparseRowPointerOffset[r] + i : i;
						index += rptr[r] + this._colID -1;
						csrblock.indexes()[index] = columnValuePairs[i].key;
						csrblock.values()[index] = columnValuePairs[i].value;
					}
				} else {
					sparseRowsWZeros.add(r);
				}
			}
		}
		if(!sparseRowsWZeros.isEmpty()) {
			addSparseRowsWZeros(sparseRowsWZeros);
		}
	}

	@Override
	protected void applyDense(CacheBlock<?> in, MatrixBlock out, int outputCol, int rowStart, int blk){
		for (int r = rowStart; r < Math.max(in.getNumRows(), rowStart + blk); r++) {
			HashMap<String, Integer> counter = countTokenAppearances(in, r);
			for (Map.Entry<String, Integer> entry : counter.entrySet())
				out.set(r, outputCol + tokenDictionary.get(entry.getKey()), entry.getValue());
		}
	}

	@NotNull
	private HashMap<String, Integer> countTokenAppearances(CacheBlock<?> in, int r) {
		String current = in.getString(r, this._colID - 1);
		HashMap<String, Integer> counter = new HashMap<>();
		if(current != null)
			for (String word : tokenize(current))
				if (!word.isEmpty()) {
					Integer old = counter.getOrDefault(word, 0);
					counter.put(word, old + 1);
				}
		return counter;
	}

	@Override
	public void allocateMetaData(FrameBlock meta) {
		meta.ensureAllocatedColumns(this.getDomainSize());
	}

	@Override
	public FrameBlock getMetaData(FrameBlock out) {
		int rowID = 0;
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, Integer> e : this.tokenDictionary.entrySet()) {
			out.set(rowID++, _colID - 1, constructRecodeMapEntry(e.getKey(), Long.valueOf(e.getValue()), sb));
		}
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
		public Void call() {
			_encoder.build(_input);
			return null;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "<ColId: " + _encoder._colID + ">";
		}

	}
}
