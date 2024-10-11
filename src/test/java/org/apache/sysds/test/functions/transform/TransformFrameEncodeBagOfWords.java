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

package org.apache.sysds.test.functions.transform;

import org.apache.sysds.common.Types;
import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.runtime.frame.data.FrameBlock;
import org.apache.sysds.runtime.matrix.data.MatrixValue;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.*;

public class TransformFrameEncodeBagOfWords extends AutomatedTestBase
{
	private final static String TEST_NAME1 = "TransformFrameEncodeBagOfWords";
	private final static String TEST_DIR = "functions/transform/";
	private final static String TEST_CLASS_DIR = TEST_DIR + TransformFrameEncodeBagOfWords.class.getSimpleName() + "/";
	private final static String DATASET = "amazonReview2023/Digital_MusicHead16k.csv";

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1));
	}

	@Test
	public void testTransformBagOfWords() {
		runTransformTest(TEST_NAME1, ExecMode.SINGLE_NODE, false, false);
	}

	@Test
	public void testTransformBagOfWordsAmazonReviews() {
		runTransformTest(TEST_NAME1, ExecMode.SINGLE_NODE, false, false, true);
	}

	@Test
	public void testTransformBagOfWordsPlusRecode() {
		runTransformTest(TEST_NAME1, ExecMode.SINGLE_NODE, true, false);
	}

	@Test
	public void testTransformBagOfWords2() {
		runTransformTest(TEST_NAME1, ExecMode.SINGLE_NODE, false, true);
	}

	@Test
	public void testTransformBagOfWordsPlusRecode2() {
		runTransformTest(TEST_NAME1, ExecMode.SINGLE_NODE, true, true);
	}

	//@Test
	public void testTransformBagOfWordsSpark() {
		runTransformTest(TEST_NAME1, ExecMode.SPARK, false, false);
	}

	private void runTransformTest(String testname, ExecMode rt, boolean recode, boolean dup){
		runTransformTest(testname, rt, recode, dup, false);
	}

	private void runTransformTest(String testname, ExecMode rt, boolean recode, boolean dup, boolean fromFile)
	{
		//set runtime platform
		ExecMode rtold = setExecMode(rt);
		try
		{
			getAndLoadTestConfiguration(testname);
			fullDMLScriptName = getScript();

			// Create the dataset by repeating and shuffling the distinct tokens
			String[] sentenceColumn = fromFile ? readReviews(DATASET_DIR + DATASET) : new String[]{"This is the first document","This document is the second document",
					"And this is the third one","Is this the first document"};
			String[] recodeColumn = recode ? new String[]{"A", "B", "A", "C"} : null;
			if(!fromFile)
				writeStringsToCsvFile(sentenceColumn, recodeColumn, baseDirectory + INPUT_DIR + "data", dup);

			programArgs = new String[]{"-stats","-args", fromFile ? DATASET_DIR + DATASET : input("data"), output("result"), output("dict"),
					String.valueOf(recode), String.valueOf(dup)};
			runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
			HashMap<MatrixValue.CellIndex, Double> res_actual = readDMLMatrixFromOutputDir("result");
			double[][] result = TestUtils.convertHashMapToDoubleArray(res_actual);
			System.out.println(baseDirectory);
			FrameBlock dict_frame = readDMLFrameFromHDFS( "dict", Types.FileFormat.CSV);
			checkResults(sentenceColumn, result, recodeColumn, dict_frame, dup);
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			resetExecMode(rtold);
		}
	}

	private String[] readReviews(String s) {
        try {
            FrameBlock in = readDMLFrameFromHDFS(s, Types.FileFormat.CSV, false);
			String[] out = new String[in.getNumRows()];
			for (int i = 0; i < in.getNumRows(); i++) {
				out[i] = in.getString(i, 0);
			}
			return out;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
	}

	public static void checkResults(String[] sentences, double[][] result, String[] recodeColumn, FrameBlock dict, boolean dup){
		HashMap<String, Integer> indices = new HashMap<>();
		for (int i = 0; i < dict.getNumRows(); i++) {
			String[] tuple = dict.getString(i, 0).split("\u00b7");
			indices.put(tuple[0], Integer.parseInt(tuple[1]));
		}
		HashMap<String, Integer> rcdMap = new HashMap<>();
		if(recodeColumn != null){
			for (int i = 0; i < dict.getNumRows(); i++) {
				String current = dict.getString(i, 1);
				if(current == null)
					break;
				String[] tuple = current.split("\u00b7");
				rcdMap.put(tuple[0], Integer.parseInt(tuple[1]));
			}
		}
		int r = 0;
		for (int i = 0; i < sentences.length; i++) {
			String sentence = sentences[i];
			HashMap<String, Integer> count = new HashMap<>();
			String[] words = sentence.split(" ");
			for (String word : words) {
				if (!word.isEmpty()) {

					word = word.toLowerCase();
					Integer old = count.getOrDefault(word, 0);
					count.put(word, old + 1);
				}
			}

			// compare results: bag of words
			for(Map.Entry<String, Integer> entry : count.entrySet()){
				String word = entry.getKey();
				int count_expected = entry.getValue();
				int index = indices.get(word);
				assert result[r][index] == count_expected;
			}

			int offset = indices.size();

			// recode:
			if(recodeColumn != null)
				assert result[r][offset] == rcdMap.get(recodeColumn[r]);
			r++;
		}
	}

	public static void writeStringsToCsvFile(String[] sentences, String[] recodeTokens, String fileName, boolean duplicate) throws IOException {
		Path path = Paths.get(fileName);
		Files.createDirectories(path.getParent());
		try (BufferedWriter bw = Files.newBufferedWriter(path)) {
			for (int i = 0; i < sentences.length; i++) {
				String out = sentences[i] +  (recodeTokens != null ? "," + recodeTokens[i] : "");
				if(duplicate)
					out = out  + ","  + out;
				bw.write(out);
				bw.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
