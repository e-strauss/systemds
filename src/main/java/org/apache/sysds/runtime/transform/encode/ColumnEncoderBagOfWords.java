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
	private HashMap<String, Long> tokenDictionary;
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
		// filter for characters that match: a-z, A-Z, 0-9
		current = current.replaceAll("'"," ");
		current = STOP_CHAR_PATTERN.matcher(this.caseSensitive ? current : current.toLowerCase())
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
		long i = 0;
		this.nnz = 0;
		nnzPerRow = new int[in.getNumRows()];
		HashSet<String> current_dictionary;
		for (int r = 0; r < in.getNumRows(); r++) {
			current_dictionary = new HashSet<>();
			String current = in.getString(r, this._colID - 1);
			if(current != null)
				for(String word : tokenize(current))
					if(!word.isEmpty()){
						if(!this.tokenDictionary.containsKey(word))
							this.tokenDictionary.put(word, i++);
						current_dictionary.add(word);
					}
			this.nnzPerRow[r] = current_dictionary.size();
			this.nnz += current_dictionary.size();
//			if(current_dictionary.size() < 10){
//				System.out.println(Arrays.toString(current_dictionary.toArray()) + " : " + current);
//			}
		}
		if(i > Integer.MAX_VALUE)
			throw new DMLRuntimeException("Token Dictionary of the BagOfWords Encoder on Col [" + this._colID +
					"] exceeds the maximum number of output columns");
	}

	protected void applySparse(CacheBlock<?> in, MatrixBlock out, int outputCol, int rowStart, int blk) {
		boolean mcsr = MatrixBlock.DEFAULT_SPARSEBLOCK == SparseBlock.Type.MCSR;
		mcsr = false; // force CSR for transformencode
		ArrayList<Integer> sparseRowsWZeros = null;
		int index = _colID - 1;
		for(int r = rowStart; r < getEndIndex(in.getNumRows(), rowStart, blk); r++) {
			if(mcsr) {
				throw new NotImplementedException();
			}
			else { // csr
				HashMap<String, Long> counter = countTokenAppearances(in, r);
				SparseBlockCSR csrblock = (SparseBlockCSR) out.getSparseBlock();
				int rptr[] = csrblock.rowPointers();

				for (Map.Entry<String, Long> entry : counter.entrySet()) {
					String word = entry.getKey();
					Long count = entry.getValue();
					long c = outputCol - 1 + tokenDictionary.get(word);
				}
				double val = csrblock.values()[rptr[r] + index];
				if(Double.isNaN(val)) {
					if(sparseRowsWZeros == null)
						sparseRowsWZeros = new ArrayList<>();
					sparseRowsWZeros.add(r);
					csrblock.values()[rptr[r] + index] = 0; // test
					continue;
				}
				// Manually fill the column-indexes and values array
				int nCol = outputCol + (int) val - 1;
				csrblock.indexes()[rptr[r] + index] = nCol;
				csrblock.values()[rptr[r] + index] = 1;
			}
		}
		if(sparseRowsWZeros != null) {
			addSparseRowsWZeros(sparseRowsWZeros);
		}
	}

	@Override
	protected void applyDense(CacheBlock<?> in, MatrixBlock out, int outputCol, int rowStart, int blk){
		for (int r = rowStart; r < Math.max(in.getNumRows(), rowStart + blk); r++) {
			HashMap<String, Long> counter = countTokenAppearances(in, r);
			for (Map.Entry<String, Long> entry : counter.entrySet()) {
				String word = entry.getKey();
				Long count = entry.getValue();
				long c = outputCol + tokenDictionary.get(word);
				out.set(r, (int) c, count);
			}
		}
	}

	@NotNull
	private HashMap<String, Long> countTokenAppearances(CacheBlock<?> in, int r) {
		String current = in.getString(r, this._colID - 1);
		HashMap<String, Long> counter = new HashMap<>();
		for (String word : tokenize(current))
			if (!word.isEmpty()) {
				Long old = counter.getOrDefault(word, 0L);
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
		for(Map.Entry<String, Long> e : this.tokenDictionary.entrySet()) {
			out.set(rowID++, _colID - 1, constructRecodeMapEntry(e.getKey(), e.getValue(), sb));
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
