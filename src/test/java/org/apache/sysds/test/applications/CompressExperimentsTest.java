package org.apache.sysds.test.applications;

import org.apache.sysds.hops.OptimizerUtils;
import org.apache.sysds.test.AutomatedTestBase;
import org.junit.Test;

import java.util.stream.Stream;

public class CompressExperimentsTest extends AutomatedTestBase {
    private final static String TEST_DIR = "applications/CompressExperiments/";
    private final static String TEST_NAME = "kmeans+";

    protected final static String ALGORITHM_DIR = "/home/elias/PycharmProjects/pythonProject/experiments/code/algorithms/";
    protected final static String DATA_DIR = "/home/elias/PycharmProjects/pythonProject/experiments/data/";
    protected final static String CONFIG_FILE = "/home/elias/IdeaProjects/systemds/target/testTemp/applications/" +
            "CompressExperiments/CompressExperimentsTest/Configs/claWorkloadb1.xml";
    protected final static String CENSUS_TRAIN = "census/train_census_enc.data";
    protected String TEST_CLASS_DIR = TEST_DIR + CompressExperimentsTest.class.getSimpleName();

    @Override
    public void setUp() {
        addTestConfiguration(TEST_CLASS_DIR, TEST_NAME);
    }

    @Test
    public void runKmeansExpBase(){
        runExp(false, TEST_NAME);
    }

    @Test
    public void runKmeansExpAware(){
        runExp(true, TEST_NAME);
    }

    public void runExp(boolean aware, String test){
        getAndLoadTestConfiguration(test);
        fullDMLScriptName = ALGORITHM_DIR + test + ".dml"; //getScript();
        String[] baseArgs = { "-seed", "3333", "-explain", "runtime", "-stats", "-args", DATA_DIR + CENSUS_TRAIN };
        programArgs = aware ? Stream.concat(Stream.of("-config", CONFIG_FILE), Stream.of(baseArgs))
                .toArray(String[]::new) : baseArgs;

        runTest(true, EXCEPTION_NOT_EXPECTED, null, -1);
    }
}
