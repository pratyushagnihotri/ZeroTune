package plangeneratorflink.querybuilder;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.graphstream.graph.Graph;
import plangeneratorflink.enumeration.EnumerationController;
import plangeneratorflink.graph.EnrichedQueryGraph;
import plangeneratorflink.graph.GraphBuilder;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.FlinkMetrics;
import plangeneratorflink.utils.experimentinterceptor.ExperimentInterceptor;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/** This class is the one that actually starts the query. */
public class QueryRunner {

    private final Configuration config;
    private final FlinkMetrics fm;
    private final GraphBuilder graphBuilder;
    private final EnumerationController enumerationController;
    private final String environment;
    private final int queryDuration;
    private final ExperimentInterceptor experimentInterceptor;

    public QueryRunner(
            Configuration config,
            FlinkMetrics fm,
            GraphBuilder graphBuilder,
            EnumerationController enumerationController,
            ExperimentInterceptor ei) {
        this.config = config;
        this.fm = fm;
        this.graphBuilder = graphBuilder;
        this.enumerationController = enumerationController;
        this.experimentInterceptor = ei;
        this.environment =
                config.getString(ConfigOptions.key("environment").stringType().noDefaultValue());
        this.queryDuration =
                config.getInteger(ConfigOptions.key("queryDuration").intType().noDefaultValue());
    }

    protected boolean enumerationControllerHasNext(String runName) {
        return enumerationController.hasNext(runName);
    }

    protected StreamGraph artificialCreateQuery(
            Configuration config,
            String runName,
            int parameterIndex,
            AbstractQueryBuilder queryBuilder)
            throws UnknownHostException {
        StreamExecutionEnvironment env = createStreamExecutionEnvironment(config);
        Graph currentGraph = queryBuilder.buildQuery(env, config, runName, parameterIndex, null);
        StreamGraph streamGraph = env.getStreamGraph();
        enumerationController.setParallelismForStreamGraph(streamGraph, currentGraph, runName);
        return streamGraph;
    }

    protected void executeQuery(
            Configuration config,
            String runName,
            int parameterIndex,
            AbstractQueryBuilder queryBuilder,
            FlinkMetrics fm,
            GraphBuilder graphBuilder)
            throws Exception {
        executeQuery(config, runName, parameterIndex, null, queryBuilder, fm, graphBuilder);
    }

    protected void executeQuery(
            Configuration config,
            String runName,
            int parameterIndex,
            String suffix,
            AbstractQueryBuilder queryBuilder,
            FlinkMetrics fm,
            GraphBuilder graphBuilder)
            throws Exception {
        StreamExecutionEnvironment env = createStreamExecutionEnvironment(config);
        if (experimentInterceptor != null) {
            experimentInterceptor.intercept(parameterIndex);
        }

        // cancel all existing running jobs to be sure that job is running for itself
        // we ignore localWithWebUI because the web ui hasn't spun up until this call and local
        // because there isn't a REST API in this case
        if (!environment.equals("localWithWebUI") && !environment.equals("local")) {
            fm.cancelAllRunningJobs(true);
            if (environment.equals("localCluster")) {
                fm.checkTaskManagerAvailability(true);
            }
            Thread.sleep(Constants.IDLE_BETWEEN_QUERYS_DURATION);
        }

        Graph currentGraph = queryBuilder.buildQuery(env, config, runName, parameterIndex, suffix);
        if (currentGraph != null) { // is null if duplicate was found
            StreamGraph streamGraph = env.getStreamGraph();
            enumerationController.setParallelismForStreamGraph(streamGraph, currentGraph, runName);
            String jobName = "";
            if (suffix == null) {
                jobName = System.getProperty("hostname") + "-" + runName + "-" + parameterIndex;
            } else {
                jobName =
                        System.getProperty("hostname")
                                + "-"
                                + runName
                                + "-"
                                + parameterIndex
                                + "-"
                                + suffix;
            }
            streamGraph.setJobName(jobName);
            env.executeAsync(streamGraph);
            Thread.sleep(queryDuration);
            if (!environment.equals("local")) {
                boolean stillRunning = fm.checkIfJobIsStillRunning();
                if (stillRunning) {
                    // fm.logGrouping(streamGraph);
                    // fm.logPlacement();
                    fm.cancelAllRunningJobs(true);
                    graphBuilder.buildEnrichedQueryGraph(
                            new EnrichedQueryGraph(
                                    streamGraph, currentGraph, currentGraph.getId()));
                    graphBuilder.writeLabelsFile(currentGraph.getId());
                }
                Thread.sleep(Constants.IDLE_BETWEEN_QUERYS_DURATION);
            }
        }
    }

    private StreamExecutionEnvironment createStreamExecutionEnvironment(Configuration config) {
        // set up the streaming execution environment
        final StreamExecutionEnvironment env;
        if (environment.equals("localCluster")) {
            env =
                    StreamExecutionEnvironment.createRemoteEnvironment(
                            System.getProperty("hostname"), 8081, config);
        } else if (environment.equals("kubernetes")
                || environment.equals("local")
                || environment.equals("localCommandLine")) {
            env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        } else if (environment.equals("localWithWebUI")) {
            env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(config);
        } else {
            throw new IllegalArgumentException(
                    "the --environment parameter needs to be 'kubernetes': uses getExecutionEnvironment() to automatically find the right environment (needed for kubernetes), 'localWithWebUI' to start local environment with web ui / rest support, local (i.e. running from IDE), 'localCluster' (i.e. if flink is started by ./bin/start-cluster.sh, (!) not for kubernetes setup), 'localCommandLine' (i.e. having a local cluster setup and running plangeneratorflink from the command line like ./bin/flink run)");
        }
        // ToDo: move setting to config setting function
        Map<String, String> globalJobParametersMap =
                env.getConfig().getGlobalJobParameters().toMap();
        globalJobParametersMap = new HashMap<>(globalJobParametersMap);
        globalJobParametersMap.put(
                "-mongoAddress",
                config.getString(ConfigOptions.key("mongo.address").stringType().noDefaultValue()));
        globalJobParametersMap.put(
                "-mongoPort",
                String.valueOf(
                        config.getInteger(
                                ConfigOptions.key("mongo.port").intType().noDefaultValue())));
        globalJobParametersMap.put(
                "-mongoDatabase",
                config.getString(
                        ConfigOptions.key("mongo.database").stringType().noDefaultValue()));
        globalJobParametersMap.put(
                "-mongoUsername",
                config.getString(
                        ConfigOptions.key("mongo.username").stringType().noDefaultValue()));
        globalJobParametersMap.put(
                "-mongoPassword",
                config.getString(
                        ConfigOptions.key("mongo.password").stringType().noDefaultValue()));
        globalJobParametersMap.put(
                "-mongoCollectionObservations",
                config.getString(
                        ConfigOptions.key("mongo.collection.observations")
                                .stringType()
                                .noDefaultValue()));
        ParameterTool pt = ParameterTool.fromMap(globalJobParametersMap);
        env.getConfig().setGlobalJobParameters(pt);
        return env;
    }

    public void resetEnumerationControllerAndExperimentInterceptor() {
        this.enumerationController.reset();
        if (experimentInterceptor != null) {
            this.experimentInterceptor.reset();
        }
    }
}
