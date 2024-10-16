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
import org.apache.sysds.api.DMLScript;
import org.apache.sysds.runtime.DMLRuntimeException;
import org.apache.sysds.runtime.controlprogram.caching.CacheBlock;
import org.apache.sysds.runtime.data.SparseBlock;
import org.apache.sysds.runtime.data.SparseBlockCSR;
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixBlock;
import org.apache.sysds.runtime.util.DependencyTask;
import org.apache.sysds.runtime.util.DependencyThreadPool;
import org.apache.sysds.utils.stats.TransformStatistics;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

import static org.apache.sysds.runtime.transform.encode.ColumnEncoderRecode.constructRecodeMapEntry;
import static org.apache.sysds.runtime.util.UtilFunctions.getBlockSizes;
import static org.apache.sysds.runtime.util.UtilFunctions.getEndIndex;

public class ColumnEncoderBagOfWords extends ColumnEncoder {

	public static int NUM_SAMPLES_MAP_ESTIMATION = 1600;
	protected int[] nnzPerRow;
	private static final Pattern STOP_CHAR_PATTERN = Pattern.compile("[^a-zA-Z\\s]"); //0-9
	private HashMap<String, Integer> tokenDictionary;
	protected String seperatorRegex = "\\s+"; // whitespace
	protected boolean caseSensitive = false;
	protected long nnz = 0;
	protected long[] nnzPartials;
	protected long accurateApplyStart;
	protected long accurateApplyEnd;
	protected long accurateBuildStart;
	protected long accurateBuildEnd;

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
		int[] sentenceLengths = new int[max_index];
		for (int i = 0; i < max_index; i++) {
			int sind = sampleIndices[i];
			String current = in.getString(sind, this._colID - 1);
			int sentenceLength = 0;
			if(current != null){
				for(String word : tokenize(current, caseSensitive, seperatorRegex))
					if(!word.isEmpty()){
						if (distinctFreq.containsKey(word))
							distinctFreq.put(word, distinctFreq.get(word) + 1);
						else {
							distinctFreq.put(word, 1);
							// Maintain total size of the keys
							totSize += (word.length() * 2L + 16); //sizeof(String) = len(chars) + header
						}
						sentenceLength++;
					}
			}
			sentenceLengths[i] = sentenceLength;
			totalNumWords += sentenceLength;
		}
		// based on a small experimental evaluation:
		// we increase the upperbound of the total count estimate by 10%
		// since the distinct token was also a bit off, which can be influenced by total token count
		// we increase the total size by another 15%, which is okay since we want to get an upper bound estimate
		double avgSentenceLength = (double) Arrays.stream(sentenceLengths).sum() / (double) sentenceLengths.length*1.25;
		estimateDistinctTokens(totalNumWords, distinctFreq, (int) (avgSentenceLength*in.getNumRows()), totSize);
	}

	@NotNull
	public static String[] tokenize(String current, boolean caseSensitive, String seperatorRegex) {
		// string builder is faster than regex
		StringBuilder finalString = new StringBuilder();
		for (char c : current.toCharArray()) {
			// Convert to lowercase if needed
			char lowerChar = caseSensitive ? c : Character.toLowerCase(c);
			// Replace ’, ', ., and / with a space
			if (lowerChar == '’' || lowerChar == '\'' || lowerChar == '.' || lowerChar == '/')
				finalString.append(' ');
				// Keep only alphabetic characters or spaces
			else if (Character.isLetter(lowerChar))
				finalString.append(lowerChar);
			else if (Character.isWhitespace(lowerChar))
				finalString.append(' ');
		}
		return finalString.toString().split(seperatorRegex);
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
		long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
		accurateBuildStart = t0;
		int i = 0;
		this.nnz = 0;
		nnzPerRow = new int[in.getNumRows()];
		HashSet<String> tokenSetPerRow;
		for (int r = 0; r < in.getNumRows(); r++) {
			tokenSetPerRow = new HashSet<>();
			String current = in.getString(r, this._colID - 1);
			if(current != null)
				for(String word : tokenize(current, caseSensitive, seperatorRegex))
					if(!word.isEmpty()){
						tokenSetPerRow.add(word);
						if(!this.tokenDictionary.containsKey(word))
							this.tokenDictionary.put(word, i++);
					}
			this.nnzPerRow[r] = tokenSetPerRow.size();
			this.nnz += tokenSetPerRow.size();
		}
		//System.out.println("totol word count: " + totalWords);
		//System.out.println("distinct count: " + tokenDictionary.size());
		// Overflow
		if(i < 0)
			throw new DMLRuntimeException("Token-Dictionary size of the BagOfWords Encoder on Col [" + this._colID +
					"] exceeds the maximum number of output columns");
		accurateBuildEnd = System.nanoTime();
		if(DMLScript.STATISTICS)
			TransformStatistics.incBagOfWordsBuildTime(System.nanoTime()-t0);
	}

	@Override
	public List<DependencyTask<?>> getBuildTasks(CacheBlock<?> in) {
		List<Callable<Object>> tasks = new ArrayList<>();
		List<List<? extends Callable<?>>> dep = null;
		int nRows = in.getNumRows();
		int[] blockSizes = getBlockSizes(nRows, _nBuildPartitions);
		if(blockSizes.length == 1) {
			tasks.add(getBuildTask(in));
		}
		else {
			this.nnzPerRow = new int[in.getNumRows()];
			this.nnzPartials = new long[blockSizes.length];
			HashMap<Integer, HashSet<String>> ret = new HashMap<>();
			for(int startRow = 0, i = 0; i < blockSizes.length; startRow+=blockSizes[i], i++)
				tasks.add(getPartialBuildTask(in, startRow, blockSizes[i], ret, nnzPerRow, nnzPartials, i));
			tasks.add(getBowPartialMergeBuildTask(ret));
			dep = new ArrayList<>(Collections.nCopies(tasks.size() - 1, null));
			dep.add(tasks.subList(0, tasks.size() - 1));
		}
		return DependencyThreadPool.createDependencyTasks(tasks, dep);
	}

	public Callable<Object> getPartialBuildTask(CacheBlock<?> in, int startRow, int blockSize,
												HashMap<Integer, HashSet<String>> ret, int[] nnzPerRow, long[] nnzPartials, int pos) {
		return new BowPartialBuildTask(in, _colID, startRow, blockSize, ret, nnzPerRow, caseSensitive, seperatorRegex, nnzPartials, pos, this);
	}

	public Callable<Object> getBowPartialMergeBuildTask(HashMap<Integer, HashSet<String>> ret) {
		return new BowMergePartialBuildTask(this, ret);
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
				HashMap<String, Integer> counter = countTokenAppearances(in, r, _colID-1, caseSensitive, seperatorRegex);
				if(counter.isEmpty())
					sparseRowsWZeros.add(r);
				else {
					SparseBlockCSR csrblock = (SparseBlockCSR) out.getSparseBlock();
					int[] rptr = csrblock.rowPointers();
					// assert that nnz from build is equal to nnz from apply
					assert counter.size() == nnzPerRow[r];
					Pair[] columnValuePairs = new Pair[counter.size()];
					int i = 0;
					for (Map.Entry<String, Integer> entry : counter.entrySet()) {
						String word = entry.getKey();
						columnValuePairs[i] = new Pair(outputCol + tokenDictionary.get(word), entry.getValue());
						i++;
					}
					// insertion sorts performs better for small arrays
					if(columnValuePairs.length >= 128)
						Arrays.sort(columnValuePairs, Comparator.comparingInt(pair -> pair.key));
					else
						insertionSort(columnValuePairs);
					// Manually fill the column-indexes and values array
					for (i = 0; i < columnValuePairs.length; i++) {
						int index = sparseRowPointerOffset != null ? sparseRowPointerOffset[r] + i : i;
						index += rptr[r] + this._colID -1;
						csrblock.indexes()[index] = columnValuePairs[i].key;
						csrblock.values()[index] = columnValuePairs[i].value;
					}
				}
			}
		}
		if(!sparseRowsWZeros.isEmpty()) {
			addSparseRowsWZeros(sparseRowsWZeros);
		}
//		if(DMLScript.STATISTICS){
//			long t = System.nanoTime();
//			synchronized (this){
//				if(this.accurateApplyEnd < t)
//					this.accurateApplyEnd = t;
//			}
//		}
	}

	private static void insertionSort(Pair[] arr) {
		for (int i = 1; i < arr.length; i++) {
			Pair current = arr[i];
			int j = i - 1;
			while (j >= 0 && arr[j].key > current.key) {
				arr[j + 1] = arr[j];
				j--;
			}
			arr[j + 1] = current;
		}
	}

	@Override
	protected void applyDense(CacheBlock<?> in, MatrixBlock out, int outputCol, int rowStart, int blk){
		for (int r = rowStart; r < Math.max(in.getNumRows(), rowStart + blk); r++) {
			HashMap<String, Integer> counter = countTokenAppearances(in, r, _colID-1, caseSensitive, seperatorRegex);
			for (Map.Entry<String, Integer> entry : counter.entrySet())
				out.set(r, outputCol + tokenDictionary.get(entry.getKey()), entry.getValue());
		}
	}

	@NotNull
	private static HashMap<String, Integer> countTokenAppearances(CacheBlock<?> in, int r, int c,
																  boolean caseSensitive, String separator) {
		String current = in.getString(r, c);
		HashMap<String, Integer> counter = new HashMap<>();
		if(current != null)
			for (String word : tokenize(current, caseSensitive, separator))
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

	private static class BowPartialBuildTask implements Callable<Object> {

		private final CacheBlock<?> _input;
		private final int _blockSize;
		private final int _startRow;
		private final int _colID;
		private final boolean _caseSensitive;
		private final String _seperator;
		private final HashMap<Integer, HashSet<String>> _partialMaps;
		private final int[] _nnzPerRow;
		private final long[] _nnzPartials;
		private final int _pos;
		private ColumnEncoderBagOfWords _enc;

		protected BowPartialBuildTask(CacheBlock<?> input, int colID, int startRow,
									  int blocksize, HashMap<Integer, HashSet<String>> partialMaps, int[] nnzPerRow,
									  boolean caseSensitive, String seperator, long[] nnzPartials, int pos, ColumnEncoderBagOfWords enc) {
			_input = input;
			_blockSize = blocksize;
			_colID = colID;
			_startRow = startRow;
			_partialMaps = partialMaps;
			_caseSensitive = caseSensitive;
			_seperator = seperator;
			_nnzPerRow = nnzPerRow;
			_nnzPartials = nnzPartials;
			_pos = pos;
			_enc = enc;
		}

		@Override
		public Object call() throws Exception {
			long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
			if(_pos == 0)
				_enc.accurateBuildStart = t0;
			int endRow = getEndIndex(_input.getNumRows(), _startRow, _blockSize);
			HashSet<String> tokenSetPartial = new HashSet<>();
			HashSet<String> tokenSetPerRow;
			long nnzPartial = 0;
			for (int r = _startRow; r < endRow; r++) {
				tokenSetPerRow = new HashSet<>();
				String current = _input.getString(r, this._colID - 1);
				if(current != null)
					for(String word : tokenize(current, _caseSensitive, _seperator))
						if(!word.isEmpty()){
							tokenSetPerRow.add(word);
							tokenSetPartial.add(word);
						}
				_nnzPerRow[r] = tokenSetPerRow.size();
				nnzPartial += tokenSetPerRow.size();
			}
			_nnzPartials[_pos] = nnzPartial;
			synchronized (_partialMaps){
				_partialMaps.put(_startRow, tokenSetPartial);
			}
			if(DMLScript.STATISTICS){
				TransformStatistics.incBagOfWordsBuildTime(System.nanoTime() - t0);
			}
			return null;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "<Start row: " + _startRow + "; Block size: " + _blockSize + ">";
		}

	}

	private static class BowMergePartialBuildTask implements Callable<Object> {
		private final HashMap<Integer, HashSet<String>> _partialMaps;
		private final ColumnEncoderBagOfWords _encoder;

		private BowMergePartialBuildTask(ColumnEncoderBagOfWords encoderRecode, HashMap<Integer, HashSet<String>> partialMaps) {
			_partialMaps = partialMaps;
			_encoder = encoderRecode;
		}

		@Override
		public Object call() throws Exception {
			long t0 = DMLScript.STATISTICS ? System.nanoTime() : 0;
			Map<String, Integer> tokenDictionary = _encoder.tokenDictionary;
			for(HashSet<String> tokenSet : _partialMaps.values()){
				tokenSet.forEach(token -> {
					if(!tokenDictionary.containsKey(token))
						tokenDictionary.put(token, tokenDictionary.size());
				});
			}
			for (long nnzPartial : _encoder.nnzPartials)
				_encoder.nnz += nnzPartial;
			_encoder.accurateBuildEnd = System.nanoTime();
			if(DMLScript.STATISTICS){
				TransformStatistics.incBagOfWordsBuildTime(System.nanoTime() - t0);
			}
			return null;
		}

		@Override
		public String toString() {
			return getClass().getSimpleName() + "<ColId: " + _encoder._colID + ">";
		}

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
