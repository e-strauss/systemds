package org.apache.sysds.test.applications;

import org.apache.sysds.test.AutomatedTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.stream.Stream;

import static org.apache.sysds.runtime.compress.workload.WorkloadAnalyzer.ALLOW_INTERMEDIATE_CANDIDATES;

public class CompressExperimentsTest extends AutomatedTestBase {
    private final static String TEST_DIR = "applications/CompressExperiments/";
    private final static String TEST_NAME1 = "kmeans+";
    private final static String TEST_NAME2 = "PCA+";
    private final static String TEST_NAME3 = "mLogReg+";
    private final static String TEST_NAME4 = "l2svm+";
    private final static String TEST_NAME5 = "lmDS+";
    private final static String TEST_NAME6 = "lmCG+";

    protected final static String ALGORITHM_DIR = "/home/elias/PycharmProjects/pythonProject/experiments/code/algorithms/";
    protected final static String DATA_DIR = "/home/elias/PycharmProjects/pythonProject/experiments/data/";
    protected final static String CONFIG_FILE = "/home/elias/IdeaProjects/systemds/target/testTemp/applications/" +
            "CompressExperiments/CompressExperimentsTest/Configs/claWorkloadb1.xml";
    protected final static String CENSUS_TRAIN = "census/train_census_enc.data";
    protected final static String CENSUS_TRAIN_LABEL = "census/train_census_enc_labels.data";
    protected final static String CENSUS_TEST = "census/train_census_enc.data";
    protected final static String CENSUS_TEST_LABEL = "census/train_census_enc_labels.data";
    protected String TEST_CLASS_DIR = TEST_DIR + CompressExperimentsTest.class.getSimpleName();
    protected static boolean SHOW_PLAN = false;

    @Override
    public void setUp() {
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME1);
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME2);
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME3);
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME4);
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME5);
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME6);
    }

    @Test
    public void runKmeansExpBase(){
        runExp(false, TEST_NAME1);
    }

    @Test
    public void runKmeansExpAware(){
        runExp(true, TEST_NAME1);
    }

    @Test
    public void runKmeansExpAwareIntermediate(){
        runExp(true, TEST_NAME1, true);
    }

    @Test
    public void runPCAExpBase(){
        runExp(false, TEST_NAME2);
    }

    @Test
    public void runPCAExpAware(){
        runExp(true, TEST_NAME2);
    }

    @Test
    public void runPCAExpAwareIntermediate(){
        runExp(true, TEST_NAME2, true);
    }

    @Test
    public void runMLogRegExpBase(){
        runExp(false, TEST_NAME3);
    }

    @Test
    public void runMLogRegExpAware(){
        runExp(true, TEST_NAME3);
    }

    @Test
    public void runMLogRegExpAwareIntermediate(){
        runExp(true, TEST_NAME3, false);
    }

    @Test
    public void runL2SVMExpBase(){
        runExp(false, TEST_NAME4);
    }

    @Test
    public void runL2SVMExpAware(){
        runExp(true, TEST_NAME4);
    }

    @Test
    public void runL2SVMExpAwareIntermediate(){
        runExp(true, TEST_NAME4, false);
    }

    @Test
    public void runLMDSExpAware(){
        runExp(true, TEST_NAME5);
    }

    @Test
    public void runLMCGExpAware(){
        runExp(true, TEST_NAME6);
    }

    public void runExp(boolean aware, String test){
        runExp(aware, test, false);
    }

    public void runExp(boolean aware, String test, boolean intermediateCompression){
        getAndLoadTestConfiguration(test);
        fullDMLScriptName = ALGORITHM_DIR + test + ".dml";
        String[] baseArgs = { "-seed", "3333",  "-stats", "-args", DATA_DIR + CENSUS_TRAIN, String.valueOf(1), "A",
                DATA_DIR + CENSUS_TRAIN_LABEL, DATA_DIR + CENSUS_TEST, DATA_DIR + CENSUS_TEST_LABEL};
        Stream<String> explain_args = SHOW_PLAN ? Stream.concat(Stream.of("-explain", "runtime"),
                Stream.of(baseArgs)) : Stream.of(baseArgs);
        programArgs = aware ? Stream.concat(Stream.of("-config", CONFIG_FILE), explain_args)
                .toArray(String[]::new) : baseArgs;
        if(intermediateCompression)
            ALLOW_INTERMEDIATE_CANDIDATES = true;

        runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
    }
}
