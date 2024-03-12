/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package plangeneratorflink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.validators.PositiveInteger;
import plangeneratorflink.enumeration.EnumerationController;
import plangeneratorflink.graph.GraphBuilder;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.querybuilder.QueryRunner;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.EnvironmentExplorer;
import plangeneratorflink.utils.FlinkMetrics;
import plangeneratorflink.utils.experimentinterceptor.ExperimentInterceptor;

import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * PlanGeneratorFlink is used for research projects in Distributed Stream Processing to generate
 * training and test data.
 */
public class PlanGeneratorFlink {
    @Parameter(
            names = {"--logdir", "-l"},
            description = "the place where to log files")
    String logDir = "./";

    /** Explicitly create remote environment. */
    @Parameter(
            names = {"--environment", "-e"},
            description =
                    "'kubernetes': uses getExecutionEnvironment() to automatically find the right environment (needed for kubernetes), 'localWithWebUI' to start local environment with web ui / rest support, local (i.e. running from IDE), 'localCluster' (i.e. if flink is started by ./bin/start-cluster.sh, (!) not for kubernetes setup), 'localCommandLine' (i.e. having a local cluster setup and running plangeneratorflink from the command line like ./bin/flink run)")
    String environment = "kubernetes";

    @Parameter(
            names = {"--debugMode", "-D"},
            description =
                    "in debugMode several things are firmly defined (i.e. seed for randomness, max count of generated source records")
    boolean debugMode;

    /** Mode of execution: Training, test or benchmark data generation. */
    @Parameter(
            names = {"--mode", "-m"},
            description =
                    "Enter one or multiple out of: train, extrapolation, randomspikedetection, filespikedetection, smartgrid, advertisement.")
    List<String> modes = new ArrayList<>();

    /** Number of randomized topologies to create. */
    @Parameter(
            names = {"--numTopos", "-n"},
            description = "number of topologies",
            validateWith = PositiveInteger.class)
    int numTopos = 100;

    /** Duration per query in ms - including warm up time. */
    @Parameter(
            names = {"--duration", "-d"},
            description = "execution time for a query",
            validateWith = PositiveInteger.class)
    int queryDuration = 60000; // 1 min

    /** EnumerationStrategy to be used. */
    @Parameter(
            names = {"--enumerationStrategy", "-p"},
            description = "defines the enumeration strategy")
    Constants.EnumerationStrategy enumerationStrategy = Constants.EnumerationStrategy.RANDOM;

    /** Parallelism to be used in combination with ParameterBasedEnumerationStrategy. */
    @Parameter(
            names = {"--parallelism", "-P"},
            description =
                    "Parallelism to be used in combination with ParameterBasedEnumerationStrategy.")
    int parallelism = -1;

    /** Minimum Parallelism to be used. Default: 1 */
    @Parameter(
            names = {"--minParallelism"},
            description = "Minimum Parallelism to be used. Default: 1")
    int minParallelism = 1;

    /** Maximum Parallelism to be used. Default: 1 */
    @Parameter(
            names = {"--maxParallelism"},
            description = "Maximum Parallelism to be used. Default: 30")
    int maxParallelism = -1;

    /** Exhaustiv Parallelism Step Size to be used. Default: 1. */
    @Parameter(
            names = {"--exhaustiveParallelismStepSize"},
            description = "Exhaustiv Parallelism Step Size to be used. Default: 1")
    int exhaustiveParallelismStepSize = 1;

    /** Use all available operators of the training queries and don't randomly omit operators. */
    @Parameter(
            names = {"--UseAllOps", "-a"},
            description =
                    "Use all available operators of the training queries and don't randomly omit operators.")
    boolean useAllOps = false;

    /** Be deterministic: Do not use randomness but a defined query (training queries only). */
    @Parameter(
            names = {"--deterministic"},
            description =
                    "Be deterministic: Do not use randomness but a defined query (training queries only).")
    boolean deterministic = false;

    /**
     * IncreasingEnumerationStrategy only: Set the step size to define how much the parallelism
     * should be increased.
     */
    @Parameter(
            names = {"--increasingStepSize"},
            description =
                    "IncreasingEnumerationStrategy only: Set the step size to define how much the parallelism should be increased.")
    int increasingStepSize = 1;

    /**
     * Defines if and what experiment interceptor should be used. Currently only eventrate is
     * available.
     */
    @Parameter(
            names = {"--interceptor"},
            description =
                    "Defines if and what experiment interceptor should be used. Currently only eventrate is available.")
    String interceptor = "";

    /** Experiment Interceptor start value. You also need to set --interceptor. */
    @Parameter(
            names = {"--interceptorStart"},
            description = "Experiment Interceptor start value. You also need to set --interceptor.")
    int interceptorStart = 0;

    /** Experiment Interceptor step value. You also need to set --interceptor. */
    @Parameter(
            names = {"--interceptorStep"},
            description = "Experiment Interceptor step value. You also need to set --interceptor.")
    int interceptorStep = 0;

    /** Define the source parallelism. */
    @Parameter(
            names = {"--sourceParallelism"},
            description = "Define the source parallelism.")
    int sourceParallelism = 0;

    /**
     * Training or Testing Templates that should be executed (Format: "--templates
     * template1,template3" (no space between)). Default: template1, template2, template3
     */
    @Parameter(
            names = {"--templates"},
            description = "Training Template that should be executed.")
    List<String> templates = new ArrayList<>();

    // Begin search heuristic parameters
    /**
     * Determine optimal parallelism set by using a search heuristic approach. The query type is
     * choosen by selecting the --mode (train or test) and the specific query by setting
     * --templates. The specific query can be defined via a file.
     */
    @Parameter(
            names = {"--searchHeuristic"},
            description =
                    "Run PlanGeneratorFlink in search heuristic mode to find optimal parallelism set. It can be choosed between random, rulebased, binarySearch and exhaustive.")
    boolean searchHeuristic = false;

    /**
     * SearchHeuristic: Search only with predictions. Do not run actual run queries. * The resulting
     * prediction calculation depends: If actual a query runs, it uses the real graph file for
     * prediction (thus including correct selectivity but also varying placement and other factors).
     * If it uses only predictions the graph files are artificially created and keep all values
     * (=placement, selectivity, ...) except the parallelism degree. Therefore they are better
     * comparable but are not 100% realistic, as e.g. selectivity can maybe change depending on the
     * computing power.
     */
    @Parameter(
            names = {"--searchHeuristicPredictions"},
            description =
                    "SearchHeuristic: Search only with predictions. Do not run actual run queries.")
    boolean searchHeuristicPredictions = false;

    /**
     * Run SearchHeuristic Prediction Comparison. This only works with searchHeuristicPredictions
     * and will use the predictions after completion to run actual querys on 100 top, 100 medium and
     * 100 bad performing queries.
     */
    @Parameter(
            names = {"--searchHeuristicPredictionComparison"},
            description =
                    "Run SearchHeuristic Prediction Comparison. This only makes sense with searchHeuristicPredictions\n"
                            + "     * and will use the predictions after completion to run actual querys on 100 top, 100 medium and\n"
                            + "     * 100 bad performing queries.")
    boolean searchHeuristicPredictionComparison = false;

    /**
     * Defines the parameters to be used for the query. You can find an explanation in the ReadMe.
     */
    @Parameter(
            names = {"--searchHeuristicFile"},
            description =
                    "Filepath to a file containing the parameters of the search heuristic query.")
    String searchHeuristicFilePath = null;

    /**
     * Defines where the zerotune-learning implementation is located, e.g.
     * /home/user/ZeroTune/zerotune-learning/flink_learning.
     */
    @Parameter(
            names = {"--searchHeuristicZerotuneLearningPath"},
            description =
                    "Defines where the zerotune-learning implementation is located, e.g. /home/user/ZeroTune/zerotune-learning/flink_learning.")
    String searchHeuristicZerotuneLearningPath = null;
    /** Defines where the folder for the model to predict query costs is located. */
    @Parameter(
            names = {"--searchHeuristicModelPath"},
            description =
                    "Filepath to a file containing the parameters of the search heuristic query.")
    String searchHeuristicModelPath = null;

    /**
     * Defines the threshold for binarySearch search heuristic. If the model is improving below the
     * threshold, search stops.
     */
    @Parameter(
            names = {"--searchHeuristicThreshold"},
            description =
                    "Defines the threshold for binarySearch search heuristic. If the model is improving below the threshold, search stops.")
    double searchHeuristicThreshold = 0.0;

    // End search heuristic parameters

    public static void main(String[] args) throws Exception {
        PlanGeneratorFlink main = new PlanGeneratorFlink();
        JCommander.newBuilder().addObject(main).build().parse(args);
        main.run();
    }

    private void run() throws Exception {
        Configuration config = setConfigParameters();
        GraphBuilder graphBuilder = new GraphBuilder(config);
        FlinkMetrics fm = new FlinkMetrics(config);
        EnumerationController enumerationController =
                new EnumerationController(config, enumerationStrategy);
        ExperimentInterceptor ei = ExperimentInterceptor.getInterceptor(config);
        QueryRunner queryRunner =
                new QueryRunner(config, fm, graphBuilder, enumerationController, ei);
        if (searchHeuristic) {
            // replace modes if we are in search heuristic mode
            modes = Collections.singletonList("searchheuristic");
        }
        AbstractQueryBuilder.createAllQueries(
                config, graphBuilder, fm, ei, modes, numTopos, queryRunner);
    }

    private Configuration setConfigParameters() throws Exception {
        Configuration config = new Configuration();
        EnvironmentExplorer ee = new EnvironmentExplorer();
        config.setString("environment", environment);
        config.setString("enumerationStrategy", enumerationStrategy.toString());
        config.setString("interceptor", interceptor);
        config.setInteger("interceptorStart", interceptorStart);
        config.setInteger("interceptorStep", interceptorStep);
        config.setInteger("queryDuration", queryDuration);
        config.setInteger("exhaustiveParallelismStepSize", exhaustiveParallelismStepSize);
        config.setInteger("numTopos", numTopos);
        config.setInteger("parallelism", parallelism);
        config.setBoolean("deterministic", deterministic);
        if (searchHeuristic) {
            config.setBoolean("searchHeuristic", searchHeuristic);
            config.setBoolean("searchHeuristicPredictions", searchHeuristicPredictions);
            config.setBoolean("searchHeuristicPredictionComparison", searchHeuristicPredictionComparison);
            config.setString("searchHeuristicZerotuneLearningPath", searchHeuristicZerotuneLearningPath);
            config.setString("searchHeuristicFilePath", searchHeuristicFilePath);
            config.setString("searchHeuristicModelPath", searchHeuristicModelPath);
            config.setDouble("searchHeuristicThreshold", searchHeuristicThreshold);
        }
        config.setInteger("increasingStepSize", increasingStepSize);
        config.setInteger("minParallelism", minParallelism);
        config.setBoolean("deterministic", deterministic);
        if (maxParallelism != -1) {
            config.setInteger("maxParallelism", maxParallelism);
        } else {
            config.setInteger("maxParallelism", 30);
        }
        if (sourceParallelism > 0) {
            config.setInteger("sourceParallelism", sourceParallelism);
        }

        if (templates.size() > 0) {
            config.setString("templates", String.join(", ", templates));
        }

        if (this.environment.equals("kubernetes")) {
            // only use autoMaxParallelism if there isn't something manually defined.
            if (this.maxParallelism == -1 && this.parallelism == -1) {
                int p = ee.determineMaxPossibleParallelism();
                config.setInteger("maxParallelism", p);
                System.out.println(
                        "auto max parallelism detection is activated and max parallelism is set to: "
                                + p);
            }
            Tuple2<String, Integer> jobManagerAddress =
                    ee.determineFlinkRestAPIAddressFromKubernetes();
            System.setProperty("hostname", jobManagerAddress.f0);
            System.setProperty("flinkRestApiPort", jobManagerAddress.f1.toString());
            Tuple2<String, Integer> mongoDBExternalAddress =
                    ee.determineMongoDBAddressFromKubernetes();
            config.setString("mongo.address", mongoDBExternalAddress.f0);
            config.setInteger("mongo.port", mongoDBExternalAddress.f1);
        } else {
            System.setProperty("hostname", InetAddress.getLocalHost().getHostName());
            System.setProperty("flinkRestApiPort", "8081");
            // address & port aren't set when we are not running on k8s
            // docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}'
            // mongodblocal_mongo_1
            Tuple2<String, Integer> mongoDBAddressFromDocker =
                    ee.determineMongoDBAddressFromDocker();
            config.setString("mongo.address", mongoDBAddressFromDocker.f0);
            config.setInteger("mongo.port", mongoDBAddressFromDocker.f1);
        }

        config.setString("mongo.database", Constants.MongoDB.DATABASENAME);
        config.setString("mongo.username", Constants.MongoDB.USERNAME);
        config.setString("mongo.password", Constants.MongoDB.PASSWORD);

        config.setString("mongo.collection.labels", Constants.MongoDB.Collection.LABELS);
        config.setString(
                "mongo.collection.observations", Constants.MongoDB.Collection.OBSERVATIONS);
        config.setString("mongo.collection.placement", Constants.MongoDB.Collection.PLACEMENT);
        config.setString("mongo.collection.graphs", Constants.MongoDB.Collection.GRAPHS);
        config.setString("mongo.collection.grouping", Constants.MongoDB.Collection.GROUPING);

        if (Files.notExists(Path.of(logDir))) {
            throw new IllegalArgumentException(
                    "logDir does not exist. Create folder '" + logDir + "' first.");
        }

        System.setProperty("debugMode", String.valueOf(debugMode));

        config.setString("logDir", logDir);
        config.setBoolean("debugMode", debugMode);
        config.setString("hostname", System.getProperty("hostname"));
        config.setInteger(
                "flinkRestApiPort", Integer.parseInt(System.getProperty("flinkRestApiPort")));
        return config;
    }
}
