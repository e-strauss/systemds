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

import org.apache.sysds.common.Types.ExecMode;
import org.apache.sysds.test.AutomatedTestBase;
import org.apache.sysds.test.TestConfiguration;
import org.apache.sysds.test.TestUtils;
import org.junit.Test;

import java.io.BufferedWriter;
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

	@Override
	public void setUp() {
		TestUtils.clearAssertionInformation();
		addTestConfiguration(TEST_NAME1, new TestConfiguration(TEST_CLASS_DIR, TEST_NAME1));
	}

	@Test
	public void testTransformBagOfWords() {
		runTransformTest(TEST_NAME1, ExecMode.SINGLE_NODE);
	}

	@Test
	public void testTransformBagOfWordsSpark() {
		runTransformTest(TEST_NAME1, ExecMode.SPARK);
	}

	private void runTransformTest(String testname, ExecMode rt)
	{
		//set runtime platform
		ExecMode rtold = setExecMode(rt);
		try
		{
			getAndLoadTestConfiguration(testname);
			fullDMLScriptName = getScript();

			// Create the dataset by repeating and shuffling the distinct tokens
			List<String> sentenceColumn = new ArrayList<>();
			sentenceColumn.add("This is the first document.");
			sentenceColumn.add("This document is the second document.");
			sentenceColumn.add("And this is the third one.");
			sentenceColumn.add("Is this the first document?");
			writeStringsToCsvFile(sentenceColumn, baseDirectory + INPUT_DIR + "data");

			programArgs = new String[]{"-stats","-args", input("data"), output("result")};
			runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
		}
		catch(Exception ex) {
			throw new RuntimeException(ex);
		}
		finally {
			resetExecMode(rtold);
		}
	}

	public static void writeStringsToCsvFile(List<String> strings, String fileName) throws IOException {
		Path path = Paths.get(fileName);
		Files.createDirectories(path.getParent());
		try (BufferedWriter bw = Files.newBufferedWriter(path)) {
			for (String line : strings) {
				bw.write(line);
				bw.newLine();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
