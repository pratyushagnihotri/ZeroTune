package plangeneratorflink.querybuilder;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import plangeneratorflink.enumeration.EnumerationController;
import plangeneratorflink.graph.GraphBuilder;
import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.operators.OperatorProvider;
import plangeneratorflink.operators.aggregate.AggregateOperator;
import plangeneratorflink.operators.filter.FilterOperator;
import plangeneratorflink.operators.join.WindowedJoinOperator;
import plangeneratorflink.operators.source.AdvertisementSourceOperator;
import plangeneratorflink.operators.source.FileSpikeDetectionSourceOperator;
import plangeneratorflink.operators.source.RandomSpikeDetectionSourceOperator;
import plangeneratorflink.operators.source.SmartGridSourceOperator;
import plangeneratorflink.operators.source.SyntheticSourceOperator;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.querybuilder.advertisement.AdvertisementQueryBuilder;
import plangeneratorflink.querybuilder.searchheuristic.ImportQueryBuilder;
import plangeneratorflink.querybuilder.smartgrid.SmartGridQueryBuilder;
import plangeneratorflink.querybuilder.spikedetection.AlternativeFileSpikeDetectionQueryBuilder;
import plangeneratorflink.querybuilder.spikedetection.SpikeDetectionQueryBuilder;
import plangeneratorflink.querybuilder.synthetic.SyntheticTestQueryBuilder;
import plangeneratorflink.querybuilder.synthetic.SyntheticTrainingQueryBuilder;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;
import plangeneratorflink.utils.FlinkMetrics;
import plangeneratorflink.utils.RanGen;
import plangeneratorflink.utils.SearchHeuristic;
import plangeneratorflink.utils.experimentinterceptor.ExperimentInterceptor;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/** Responsible as top level class to build queries. */
public abstract class AbstractQueryBuilder {
    protected final ArrayList<String> linearizedQueries = new ArrayList<>();
    protected AbstractOperator<?> curGraphHead; // current head operator of the graph
    protected int currentOperatorIndex; // index of the current operator
    protected int currentEdgeIndex;
    protected SingleGraph currentGraph;
    protected String currentQueryName;
    protected OperatorProvider operatorProvider;
    protected Configuration config;
    protected StreamExecutionEnvironment env;

    public static void createAllQueries(
            Configuration config,
            GraphBuilder graphBuilder,
            FlinkMetrics fm,
            ExperimentInterceptor ei,
            List<String> modes,
            int numTopos,
            QueryRunner queryRunner)
            throws Exception {
        String currentQueryName;
        for (String mode : modes) {
            System.out.println("############## Mode: " + mode + " ##############");
            switch (mode) {
                case Constants.Modes.TRAIN:
                    SyntheticTrainingQueryBuilder syntheticTrainingQueryBuilder =
                            new SyntheticTrainingQueryBuilder();
                    List<String> templates;
                    boolean deterministic =
                            config.getBoolean(
                                    ConfigOptions.key("deterministic")
                                            .booleanType()
                                            .defaultValue(false));
                    if (deterministic) {
                        templates = Constants.Synthetic.Train.DeterministicParameter.TEMPLATES;
                    } else {
                        templates = Constants.Synthetic.Train.TEMPLATES;
                    }
                    templates =
                            Arrays.asList(
                                    config.getString(
                                                    ConfigOptions.key("templates")
                                                            .stringType()
                                                            .defaultValue(
                                                                    String.join(", ", templates)))
                                            .split("\\s*,\\s*"));
                    for (String runName : templates) {
                        System.out.println("######## Template: " + runName + " ########");
                        queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                        for (int i = 0; i < numTopos; i++) {
                            System.out.println("### Topo: " + (i + 1) + " of " + numTopos + " ###");
                            queryRunner.executeQuery(
                                    config,
                                    runName,
                                    i,
                                    syntheticTrainingQueryBuilder,
                                    fm,
                                    graphBuilder);
                        }
                    }

                    break;

                case Constants.Modes.TEST:
                    SyntheticTestQueryBuilder syntheticTestQueryBuilder =
                            new SyntheticTestQueryBuilder();
                    templates =
                            Arrays.asList(
                                    config.getString(
                                                    ConfigOptions.key("templates")
                                                            .stringType()
                                                            .defaultValue(
                                                                    String.join(
                                                                            ", ",
                                                                            Constants.Synthetic.Test
                                                                                    .TEMPLATES)))
                                            .split("\\s*,\\s*"));
                    for (String testRun : templates) {
                        System.out.println("######## Template type: " + testRun + " ########");
                        if (testRun.equals(Constants.Synthetic.Test.TESTA)) {
                            for (int tupleWidth : Constants.Synthetic.Test.TUPLEWIDTHS) {
                                queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                                System.out.println(
                                        "###### Tuple width: "
                                                + (tupleWidth)
                                                + " of "
                                                + Arrays.toString(
                                                        Constants.Synthetic.Test.TUPLEWIDTHS)
                                                + " ######");
                                for (int i = 0; i < numTopos; i++) {
                                    System.out.println(
                                            "#### Topo run: "
                                                    + (i + 1)
                                                    + " of "
                                                    + numTopos
                                                    + " ####");
                                    for (int j = 1; j <= 3; j++) { // template 1-3
                                        System.out.println("## Template: " + j + " out of 3 ##");
                                        queryRunner.executeQuery(
                                                config,
                                                testRun
                                                        + "-template"
                                                        + j
                                                        + "-tupleWidth-"
                                                        + tupleWidth,
                                                i,
                                                syntheticTestQueryBuilder,
                                                fm,
                                                graphBuilder);
                                    }
                                }
                            }
                        } else if (testRun.equals(Constants.Synthetic.Test.TESTB)) {
                            for (int eventRates : Constants.Synthetic.Test.EVENT_RATES) {
                                queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                                System.out.println(
                                        "###### Event rate: "
                                                + (eventRates)
                                                + " of "
                                                + Arrays.toString(
                                                        Constants.Synthetic.Test.EVENT_RATES)
                                                + " ######");
                                for (int i = 0; i < numTopos; i++) {
                                    System.out.println(
                                            "#### Topo run: "
                                                    + (i + 1)
                                                    + " of "
                                                    + numTopos
                                                    + " ####");
                                    for (int j = 1; j <= 3; j++) { // template 1-3
                                        System.out.println("## Template: " + j + " out of 3 ##");
                                        queryRunner.executeQuery(
                                                config,
                                                testRun
                                                        + "-template"
                                                        + j
                                                        + "-eventRate-"
                                                        + eventRates,
                                                i,
                                                syntheticTestQueryBuilder,
                                                fm,
                                                graphBuilder);
                                    }
                                }
                            }
                        } else if (testRun.equals(Constants.Synthetic.Test.TESTC)) {
                            for (int windowDuration : Constants.Synthetic.Test.WINDOW_DURATION) {
                                queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                                System.out.println(
                                        "###### Window Duration: "
                                                + (windowDuration)
                                                + " of "
                                                + Arrays.toString(
                                                        Constants.Synthetic.Test.WINDOW_DURATION)
                                                + " ######");
                                for (int i = 0; i < numTopos; i++) {
                                    System.out.println(
                                            "#### Topo run: "
                                                    + (i + 1)
                                                    + " of "
                                                    + numTopos
                                                    + " ####");
                                    for (int j = 1; j <= 3; j++) { // template 1-3
                                        System.out.println("## Template: " + j + " out of 3 ##");
                                        queryRunner.executeQuery(
                                                config,
                                                testRun
                                                        + "-template"
                                                        + j
                                                        + "-window-duration-"
                                                        + windowDuration,
                                                i,
                                                syntheticTestQueryBuilder,
                                                fm,
                                                graphBuilder);
                                    }
                                }
                            }
                        } else if (testRun.equals(Constants.Synthetic.Test.TESTD)) {
                            for (int windowLength : Constants.Synthetic.Test.WINDOW_LENGTH) {
                                queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                                System.out.println(
                                        "###### Window Length: "
                                                + (windowLength)
                                                + " of "
                                                + Arrays.toString(
                                                        Constants.Synthetic.Test.WINDOW_LENGTH)
                                                + " ######");
                                for (int i = 0; i < numTopos; i++) {
                                    System.out.println(
                                            "#### Topo run: "
                                                    + (i + 1)
                                                    + " of "
                                                    + numTopos
                                                    + " ####");
                                    for (int j = 1; j <= 3; j++) { // template 1-3
                                        System.out.println("## Template: " + j + " out of 3 ##");
                                        queryRunner.executeQuery(
                                                config,
                                                testRun
                                                        + "-template"
                                                        + j
                                                        + "-window-length-"
                                                        + windowLength,
                                                i,
                                                syntheticTestQueryBuilder,
                                                fm,
                                                graphBuilder);
                                    }
                                }
                            }
                        } else if (testRun.equals(Constants.Synthetic.Test.TESTE)) {
                            queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                            for (int i = 0; i < numTopos; i++) {
                                System.out.println(
                                        "#### Topo run: " + (i + 1) + " of " + numTopos + " ####");
                                System.out.println("## Template: 4-way-join ##");
                                queryRunner.executeQuery(
                                        config,
                                        testRun + "-4wayjoin",
                                        i,
                                        syntheticTestQueryBuilder,
                                        fm,
                                        graphBuilder);
                                System.out.println("## Template: 5-way-join ##");
                                queryRunner.executeQuery(
                                        config,
                                        testRun + "-5wayjoin",
                                        i,
                                        syntheticTestQueryBuilder,
                                        fm,
                                        graphBuilder);
                                System.out.println("## Template: 6-way-join ##");
                                queryRunner.executeQuery(
                                        config,
                                        testRun + "-6wayjoin",
                                        i,
                                        syntheticTestQueryBuilder,
                                        fm,
                                        graphBuilder);
                                System.out.println("## Template: two-filters ##");
                                queryRunner.executeQuery(
                                        config,
                                        testRun + "-twoFilters",
                                        i,
                                        syntheticTestQueryBuilder,
                                        fm,
                                        graphBuilder);
                                System.out.println("## Template: three-filters ##");
                                queryRunner.executeQuery(
                                        config,
                                        testRun + "-threeFilters",
                                        i,
                                        syntheticTestQueryBuilder,
                                        fm,
                                        graphBuilder);
                                System.out.println("## Template: four-filters ##");
                                queryRunner.executeQuery(
                                        config,
                                        testRun + "-fourFilters",
                                        i,
                                        syntheticTestQueryBuilder,
                                        fm,
                                        graphBuilder);
                            }
                        }
                    }
                    break;
                case Constants.Modes.RandomSD:
                    SpikeDetectionQueryBuilder randomSpikeDetectionQueryBuilder =
                            new SpikeDetectionQueryBuilder();
                    queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                    for (Integer throughput : Constants.SD.throughputs) {
                        System.out.println("### Throughput: " + throughput + " ###");
                        for (int i = 0; i < numTopos; i++) {
                            System.out.println(
                                    "## Topo run: " + (i + 1) + " of " + numTopos + " ##");
                            queryRunner.executeQuery(
                                    config,
                                    "randomSpikeDetection",
                                    throughput,
                                    String.valueOf(i),
                                    randomSpikeDetectionQueryBuilder,
                                    fm,
                                    graphBuilder);
                        }
                    }
                    break;

                case Constants.Modes.FileSD:
                    SpikeDetectionQueryBuilder fileSpikeDetectionQueryBuilder =
                            new SpikeDetectionQueryBuilder();
                    queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                    for (int i = 0; i < numTopos; i++) {
                        System.out.println("### Topo run: " + (i + 1) + " of " + numTopos + " ###");
                        queryRunner.executeQuery(
                                config,
                                "fileSpikeDetection",
                                0,
                                String.valueOf(i),
                                fileSpikeDetectionQueryBuilder,
                                fm,
                                graphBuilder);
                    }
                    break;

                case Constants.Modes.AlternativeSD:
                    AlternativeFileSpikeDetectionQueryBuilder
                            alternativeSpikeDetectionQueryBuilder =
                                    new AlternativeFileSpikeDetectionQueryBuilder();
                    queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                    for (int i = 0; i < numTopos; i++) {
                        System.out.println("### Topo run: " + (i + 1) + " of " + numTopos + " ###");
                        queryRunner.executeQuery(
                                config,
                                "alternativeSpikeDetection",
                                0,
                                String.valueOf(i),
                                alternativeSpikeDetectionQueryBuilder,
                                fm,
                                graphBuilder);
                    }

                    break;

                case Constants.Modes.SG:
                    SmartGridQueryBuilder smartGridQueryBuilder = new SmartGridQueryBuilder();
                    queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                    for (Integer throughput : Constants.SG.throughputs) {
                        for (int i = 0; i < numTopos; i++) {
                            System.out.println(
                                    "### Topo run: " + (i + 1) + " of " + numTopos + " ###");
                            queryRunner.executeQuery(
                                    config,
                                    "smartGrid-global",
                                    throughput,
                                    String.valueOf(i),
                                    smartGridQueryBuilder,
                                    fm,
                                    graphBuilder);
                            queryRunner.executeQuery(
                                    config,
                                    "smartGrid-local",
                                    throughput,
                                    String.valueOf(i),
                                    smartGridQueryBuilder,
                                    fm,
                                    graphBuilder);
                        }
                    }
                    break;

                case Constants.Modes.AD:
                    AdvertisementQueryBuilder advertisementQueryBuilder =
                            new AdvertisementQueryBuilder();
                    queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                    for (Integer throughput : Constants.AD.throughputs) {
                        for (int i = 0; i < numTopos; i++) {
                            System.out.println(
                                    "### Topo run: " + (i + 1) + " of " + numTopos + " ###");
                            queryRunner.executeQuery(
                                    config,
                                    "advertisement",
                                    throughput,
                                    String.valueOf(i),
                                    advertisementQueryBuilder,
                                    fm,
                                    graphBuilder);
                        }
                    }
                    break;

                case Constants.Modes.SH:
                    // import query
                    // build for loop out of condition
                    // run query
                    ImportQueryBuilder importQueryBuilder = new ImportQueryBuilder();
                    queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                    String searchHeuristic =
                            config.getString(
                                    ConfigOptions.key("searchHeuristic")
                                            .stringType()
                                            .noDefaultValue());
                    boolean searchHeuristicPredictions =
                            config.getBoolean(
                                    ConfigOptions.key("searchHeuristicPredictions")
                                            .booleanType()
                                            .defaultValue(false));
                    boolean searchHeuristicPredictionComparison =
                            config.getBoolean(
                                    ConfigOptions.key("searchHeuristicPredictionComparison")
                                            .booleanType()
                                            .defaultValue(false));
                    String logFolder =
                            config.getString(
                                    ConfigOptions.key("logDir").stringType().defaultValue(""));
                    String searchHeuristicFilePath =
                            config.getString(
                                    ConfigOptions.key("searchHeuristicFilePath")
                                            .stringType()
                                            .noDefaultValue());
                    SearchHeuristic sh =
                            new SearchHeuristic(config, logFolder, searchHeuristicPredictions);
                    String enumerationStrategy =
                            config.getString(
                                    ConfigOptions.key("enumerationStrategy")
                                            .stringType()
                                            .noDefaultValue());
                    boolean ignoreNumTopos = false;
                    switch (enumerationStrategy) {
                        case "EXHAUSTIV":
                            numTopos = 0;
                            ignoreNumTopos = true;
                            break;
                            //                        case "RULEBASED":
                            //                            numTopos = 1;
                    }
                    System.out.println("ignoreNumTopos: " + ignoreNumTopos);
                    for (int i = 0; (i < numTopos || ignoreNumTopos); i++) {
                        System.out.println("### Topo run: " + (i + 1) + " of " + numTopos + " ###");
                        // break if enumeration has ended (e.g. no exhaustive parallelism
                        // combinations left
                        if (!queryRunner.enumerationControllerHasNext(
                                searchHeuristicPredictions
                                        ? "searchHeuristicArtificial"
                                        : "searchHeuristic")) {
                            break;
                        }
                        if (searchHeuristicPredictions) {
                            StreamGraph artificialStreamGraph =
                                    queryRunner.artificialCreateQuery(
                                            config,
                                            "searchHeuristicArtificial",
                                            i,
                                            importQueryBuilder);
                            sh.createArtificalGraphFile(
                                    searchHeuristicFilePath,
                                    logFolder,
                                    "searchHeuristicArtificial-" + i,
                                    sh.getParallelismSet(artificialStreamGraph));
                        } else {
                            queryRunner.executeQuery(
                                    config,
                                    "searchHeuristic",
                                    i,
                                    importQueryBuilder,
                                    fm,
                                    graphBuilder);
                        }
                    }
                    sh.generatePrediction();
                    sh.determineOptimalCosts();
                    // run search heuristic prediction comparison if selected
                    if (searchHeuristicPredictionComparison) {
                        EnumerationController enumerationController =
                                new EnumerationController(
                                        config,
                                        Constants.EnumerationStrategy.SHPREDICTIVECOMPARISON);
                        queryRunner =
                                new QueryRunner(
                                        config, fm, graphBuilder, enumerationController, ei);
                        queryRunner.resetEnumerationControllerAndExperimentInterceptor();
                        System.out.println("Run search heuristic prediction comparison");
                        int i = 0;
                        while (true) {
                            System.out.println(
                                    "### Topo run: " + (++i) + " of " + numTopos + " ###");
                            // break if prediction comparison has ended
                            if (!queryRunner.enumerationControllerHasNext("searchHeuristicPredictionComparison")) {
                                break;
                            }
                            queryRunner.executeQuery(
                                    config,
                                    "searchHeuristicPredictionComparison",
                                    enumerationController.getNextIndex(),
                                    importQueryBuilder,
                                    fm,
                                    graphBuilder);
                        }
                        sh.generatePredictiveComparisonCSVFile();
                    }
                    break;

                default:
                    break;
            }
        }
        graphBuilder.closeMongoDBConnection();
    }

    public static Object getKey(Class keyBasedClass, DataTuple dt) {
        return getKey(keyBasedClass, dt, 0);
    }

    public static Object getKey(Class keyBasedClass, DataTuple dt, int index) {
        {
            switch (keyBasedClass.getSimpleName()) {
                case "Integer":
                    return (Integer) dt.getValue(keyBasedClass, index)
                            * dt.getTimestamp().charAt(dt.getTimestamp().length() - 1);
                case "Double":
                    return (Double) dt.getValue(keyBasedClass, index)
                            * dt.getTimestamp().charAt(dt.getTimestamp().length() - 1);
                case "String":
                    return (String) dt.getValue(keyBasedClass, index)
                            + dt.getTimestamp().charAt(dt.getTimestamp().length() - 1);
                default:
                    throw new IllegalArgumentException(
                            "joinBasedClass " + keyBasedClass + " not supported.");
            }
        }
    }

    public abstract SingleGraph buildQuery(
            StreamExecutionEnvironment env,
            Configuration config,
            String template,
            int queryNumber,
            String suffix)
            throws UnknownHostException;

    protected void resetBuilder(
            StreamExecutionEnvironment env,
            Configuration config,
            String template,
            int queryNumber,
            String suffix)
            throws UnknownHostException {
        // ToDo: implement check for duplicates
        this.config = config;
        this.env = env;
        if (suffix == null) {
            currentQueryName = System.getProperty("hostname") + "-" + template + "-" + queryNumber;
        } else {
            currentQueryName =
                    System.getProperty("hostname")
                            + "-"
                            + template
                            + "-"
                            + queryNumber
                            + "-"
                            + suffix;
        }

        this.operatorProvider = new OperatorProvider();
        currentGraph = new SingleGraph(currentQueryName);
        curGraphHead = null;
        currentOperatorIndex = 0;
        currentEdgeIndex = 0;
    }

    protected boolean checkForDuplicates() {
        // TODO: include also parallelism in this function (its not in the description at this
        // point)
        StringBuilder stringBuilder = new StringBuilder();
        for (Node currentNode : currentGraph) {
            HashMap<String, String> nodeAttributes = new HashMap<>();
            for (Object key : (currentNode.attributeKeys().toArray())) {
                String val = String.valueOf(currentNode.getAttribute((String) key));
                nodeAttributes.put((String) key, val);
            }
            nodeAttributes.remove("id");
            stringBuilder.append(nodeAttributes);
        }
        if (linearizedQueries.contains(stringBuilder.toString())) {
            System.out.print("Duplicate found");
            System.out.print(stringBuilder + "\n");
            return false;
        } else {
            linearizedQueries.add(stringBuilder.toString());
            return true;
        }
    }

    protected SingleOutputStreamOperator<DataTuple> createRandomSpikeDetectionSource(
            int throughput) {
        RandomSpikeDetectionSourceOperator sourceOperator =
                new RandomSpikeDetectionSourceOperator(getOperatorIndex(true), throughput);
        addQueryVertex(sourceOperator);
        curGraphHead = sourceOperator;
        return this.env
                .addSource(sourceOperator.getFunction())
                .name(sourceOperator.getId())
                .returns(DataTuple.class);
    }

    protected SingleOutputStreamOperator<DataTuple> createFileSpikeDetectionSource() {
        FileSpikeDetectionSourceOperator sourceOperator =
                new FileSpikeDetectionSourceOperator(
                        getOperatorIndex(true),
                        "https://github.com/pratyushagnihotri/ZeroTune/releases/download/zerotune_benchmark_release/spikedetectiondata.gz");
        addQueryVertex(sourceOperator);
        curGraphHead = sourceOperator;
        return this.env
                .addSource(sourceOperator.getFunction())
                .name(sourceOperator.getId())
                .returns(DataTuple.class);
    }

    protected SingleOutputStreamOperator<DataTuple> createSyntheticSource() {
        return createSyntheticSource(null, null);
    }

    protected SingleOutputStreamOperator<DataTuple> createSyntheticSource(boolean deterministic) {
        Integer eventRate = null;
        Integer tupleWidth = null;
        if (deterministic) {
            eventRate = Constants.Synthetic.Train.DeterministicParameter.EVENT_RATE;
            tupleWidth = Constants.Synthetic.Train.DeterministicParameter.TUPLE_WIDTH;
        }
        return createSyntheticSource(eventRate, tupleWidth);
    }

    protected SingleOutputStreamOperator<DataTuple> createSyntheticSource(
            Integer eventRate, Integer tupleWidth) {
        return createSyntheticSource(eventRate, tupleWidth, tupleWidth, tupleWidth);
    }

    protected SingleOutputStreamOperator<DataTuple> createSyntheticSource(
            Integer eventRate,
            Integer tupleWidthInteger,
            Integer tupleWidthDouble,
            Integer tupleWidthString) {
        int configEventRate =
                config.getInteger(ConfigOptions.key("eventRate").intType().defaultValue(-1));
        if (configEventRate
                > 0) { // if defined override event rate with program parameter event rate
            eventRate = configEventRate;
        }
        SyntheticSourceOperator sourceOperator =
                new SyntheticSourceOperator(
                        getOperatorIndex(true),
                        eventRate,
                        tupleWidthInteger,
                        tupleWidthDouble,
                        tupleWidthString);
        addQueryVertex(sourceOperator);
        curGraphHead = sourceOperator;
        return this.env
                .addSource(sourceOperator.getFunction())
                .name(sourceOperator.getId())
                .returns(DataTuple.class);
    }

    protected SingleOutputStreamOperator<DataTuple> createSmartGridSource(int throughput) {
        SmartGridSourceOperator sourceOperator =
                new SmartGridSourceOperator(getOperatorIndex(true), throughput);
        addQueryVertex(sourceOperator);
        curGraphHead = sourceOperator;
        return this.env
                .addSource(sourceOperator.getFunction())
                .name(sourceOperator.getId())
                .returns(DataTuple.class);
    }

    protected SingleOutputStreamOperator<DataTuple> createAdvertisementSource(
            int throughput, Constants.AD.SourceType sourceType) {
        AdvertisementSourceOperator sourceOperator =
                new AdvertisementSourceOperator(
                        getOperatorIndex(true),
                        "https://github.com/pratyushagnihotri/ZeroTune/releases/download/zerotune_benchmark_release/ad-clicks.dat",
                        throughput,
                        sourceType);
        addQueryVertex(sourceOperator);
        curGraphHead = sourceOperator;
        return this.env
                .addSource(sourceOperator.getFunction())
                .name(sourceOperator.getId())
                .returns(DataTuple.class);
    }

    protected SingleOutputStreamOperator<DataTuple> twoWayJoin(
            DataStream<DataTuple> s0,
            DataStream<DataTuple> s1,
            String h0,
            String h1,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator,
            Class joinBasedClass) {
        return twoWayJoin(s0, s1, h0, h1, windowOperator, joinBasedClass, false);
    }

    protected SingleOutputStreamOperator<DataTuple> twoWayJoin(
            DataStream<DataTuple> s0,
            DataStream<DataTuple> s1,
            String h0,
            String h1,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator,
            Class joinBasedClass,
            boolean deterministic) {
        String operatorIndex = getOperatorIndex(true);
        if (windowOperator == null) {
            windowOperator =
                    (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                            operatorProvider.provideOperator(
                                    Constants.Operators.WINDOW, operatorIndex, deterministic);
        }
        windowOperator.setId(operatorIndex);

        if (joinBasedClass == null) {
            if (deterministic) {
                joinBasedClass = Constants.Synthetic.Train.DeterministicParameter.JOIN_BASED_CLASS;
            } else {
                joinBasedClass = RanGen.randClass();
            }
        }
        // collect descriptions from map and window operator
        // windowedJoinDescription.put(Constants.Features.joinKeyClass.name(),
        // mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        HashMap<String, Object> windowedJoinDescription =
                new HashMap<>(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin =
                new WindowedJoinOperator(operatorIndex, windowedJoinDescription);
        windowedJoin.setJoinKeyClass(joinBasedClass);

        // build graph
        addQueryVertex(windowedJoin);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h0),
                currentGraph.getNode(windowedJoin.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h1),
                currentGraph.getNode(windowedJoin.getId()));
        curGraphHead = windowedJoin;

        Class finalJoinBasedClass = joinBasedClass;
        return executeJoin(
                s0,
                s1,
                dt0 -> getKey(finalJoinBasedClass, dt0),
                dt1 -> getKey(finalJoinBasedClass, dt1),
                windowOperator,
                windowedJoin,
                operatorIndex);
    }

    protected SingleOutputStreamOperator<DataTuple> threeWayJoin(
            DataStream<DataTuple> s0,
            DataStream<DataTuple> s1,
            DataStream<DataTuple> s2,
            String h0,
            String h1,
            String h2,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator) {
        return threeWayJoin(s0, s1, s2, h0, h1, h2, windowOperator, false);
    }

    protected SingleOutputStreamOperator<DataTuple> threeWayJoin(
            DataStream<DataTuple> s0,
            DataStream<DataTuple> s1,
            DataStream<DataTuple> s2,
            String h0,
            String h1,
            String h2,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator,
            boolean deterministic) {
        String operatorIndex1 = getOperatorIndex(true);
        String operatorIndex2 = getOperatorIndex(true);
        if (windowOperator == null) {
            windowOperator =
                    (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                            operatorProvider.provideOperator(
                                    Constants.Operators.WINDOW, operatorIndex1);
        }

        Class joinBasedClass = null;
        if (deterministic) {
            joinBasedClass = Constants.Synthetic.Train.DeterministicParameter.JOIN_BASED_CLASS;
        } else {
            joinBasedClass = RanGen.randClass();
        }
        // collect descriptions from map and window operator
        // windowedJoinDescription.put(Constants.Features.joinKeyClass.name(),
        // mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        HashMap<String, Object> windowedJoinDescription =
                new HashMap<>(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin1 =
                new WindowedJoinOperator(operatorIndex1, windowedJoinDescription);
        WindowedJoinOperator windowedJoin2 =
                new WindowedJoinOperator(operatorIndex2, windowedJoinDescription);
        windowedJoin1.setJoinKeyClass(joinBasedClass);
        windowedJoin2.setJoinKeyClass(joinBasedClass);

        // build graph
        addQueryVertex(windowedJoin1);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h0),
                currentGraph.getNode(windowedJoin1.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h1),
                currentGraph.getNode(windowedJoin1.getId()));
        curGraphHead = windowedJoin1;

        addQueryVertex(windowedJoin2);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin1.getId()),
                currentGraph.getNode(windowedJoin2.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h2),
                currentGraph.getNode(windowedJoin2.getId()));
        curGraphHead = windowedJoin2;

        Class finalJoinBasedClass = joinBasedClass;
        SingleOutputStreamOperator<DataTuple> joinedStream1 =
                executeJoin(
                        s0,
                        s1,
                        dt0 -> getKey(finalJoinBasedClass, dt0),
                        dt1 -> getKey(finalJoinBasedClass, dt1),
                        windowOperator,
                        windowedJoin1,
                        operatorIndex1);

        return executeJoin(
                joinedStream1,
                s2,
                dt0 -> getKey(finalJoinBasedClass, dt0),
                dt1 -> getKey(finalJoinBasedClass, dt1),
                windowOperator,
                windowedJoin2,
                operatorIndex2);
    }

    protected SingleOutputStreamOperator<DataTuple> fourWayJoin(
            DataStream<DataTuple> s0,
            DataStream<DataTuple> s1,
            DataStream<DataTuple> s2,
            DataStream<DataTuple> s3,
            String h0,
            String h1,
            String h2,
            String h3,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator) {
        String operatorIndex1 = getOperatorIndex(true);
        String operatorIndex2 = getOperatorIndex(true);
        String operatorIndex3 = getOperatorIndex(true);
        if (windowOperator == null) {
            windowOperator =
                    (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                            operatorProvider.provideOperator(
                                    Constants.Operators.WINDOW, operatorIndex1);
        }

        Class joinBasedClass = RanGen.randClass();
        // collect descriptions from map and window operator
        // windowedJoinDescription.put(Constants.Features.joinKeyClass.name(),
        // mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        HashMap<String, Object> windowedJoinDescription =
                new HashMap<>(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin1 =
                new WindowedJoinOperator(operatorIndex1, windowedJoinDescription);
        WindowedJoinOperator windowedJoin2 =
                new WindowedJoinOperator(operatorIndex2, windowedJoinDescription);
        WindowedJoinOperator windowedJoin3 =
                new WindowedJoinOperator(operatorIndex3, windowedJoinDescription);
        windowedJoin1.setJoinKeyClass(joinBasedClass);
        windowedJoin2.setJoinKeyClass(joinBasedClass);
        windowedJoin3.setJoinKeyClass(joinBasedClass);

        // build graph
        addQueryVertex(windowedJoin1);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h0),
                currentGraph.getNode(windowedJoin1.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h1),
                currentGraph.getNode(windowedJoin1.getId()));
        curGraphHead = windowedJoin1;

        addQueryVertex(windowedJoin2);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin1.getId()),
                currentGraph.getNode(windowedJoin2.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h2),
                currentGraph.getNode(windowedJoin2.getId()));
        curGraphHead = windowedJoin2;

        addQueryVertex(windowedJoin3);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin2.getId()),
                currentGraph.getNode(windowedJoin3.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h3),
                currentGraph.getNode(windowedJoin3.getId()));
        curGraphHead = windowedJoin3;

        SingleOutputStreamOperator<DataTuple> joinedStream1 =
                executeJoin(
                        s0,
                        s1,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin1,
                        operatorIndex1);

        SingleOutputStreamOperator<DataTuple> joinedStream2 =
                executeJoin(
                        joinedStream1,
                        s2,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin2,
                        operatorIndex2);
        return executeJoin(
                joinedStream2,
                s3,
                dt0 -> getKey(joinBasedClass, dt0),
                dt1 -> getKey(joinBasedClass, dt1),
                windowOperator,
                windowedJoin3,
                operatorIndex3);
    }

    protected SingleOutputStreamOperator<DataTuple> fiveWayJoin(
            DataStream<DataTuple> s0,
            DataStream<DataTuple> s1,
            DataStream<DataTuple> s2,
            DataStream<DataTuple> s3,
            DataStream<DataTuple> s4,
            String h0,
            String h1,
            String h2,
            String h3,
            String h4,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator) {
        String operatorIndex1 = getOperatorIndex(true);
        String operatorIndex2 = getOperatorIndex(true);
        String operatorIndex3 = getOperatorIndex(true);
        String operatorIndex4 = getOperatorIndex(true);
        if (windowOperator == null) {
            windowOperator =
                    (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                            operatorProvider.provideOperator(
                                    Constants.Operators.WINDOW, operatorIndex1);
        }

        Class joinBasedClass = RanGen.randClass();
        // collect descriptions from map and window operator
        // windowedJoinDescription.put(Constants.Features.joinKeyClass.name(),
        // mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        HashMap<String, Object> windowedJoinDescription =
                new HashMap<>(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin1 =
                new WindowedJoinOperator(operatorIndex1, windowedJoinDescription);
        WindowedJoinOperator windowedJoin2 =
                new WindowedJoinOperator(operatorIndex2, windowedJoinDescription);
        WindowedJoinOperator windowedJoin3 =
                new WindowedJoinOperator(operatorIndex3, windowedJoinDescription);
        WindowedJoinOperator windowedJoin4 =
                new WindowedJoinOperator(operatorIndex4, windowedJoinDescription);
        windowedJoin1.setJoinKeyClass(joinBasedClass);
        windowedJoin2.setJoinKeyClass(joinBasedClass);
        windowedJoin3.setJoinKeyClass(joinBasedClass);
        windowedJoin4.setJoinKeyClass(joinBasedClass);

        // build graph
        addQueryVertex(windowedJoin1);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h0),
                currentGraph.getNode(windowedJoin1.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h1),
                currentGraph.getNode(windowedJoin1.getId()));
        curGraphHead = windowedJoin1;

        addQueryVertex(windowedJoin2);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin1.getId()),
                currentGraph.getNode(windowedJoin2.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h2),
                currentGraph.getNode(windowedJoin2.getId()));
        curGraphHead = windowedJoin2;

        addQueryVertex(windowedJoin3);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin2.getId()),
                currentGraph.getNode(windowedJoin3.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h3),
                currentGraph.getNode(windowedJoin3.getId()));
        curGraphHead = windowedJoin3;

        addQueryVertex(windowedJoin4);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin3.getId()),
                currentGraph.getNode(windowedJoin4.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h4),
                currentGraph.getNode(windowedJoin4.getId()));
        curGraphHead = windowedJoin4;

        SingleOutputStreamOperator<DataTuple> joinedStream1 =
                executeJoin(
                        s0,
                        s1,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin1,
                        operatorIndex1);

        SingleOutputStreamOperator<DataTuple> joinedStream2 =
                executeJoin(
                        joinedStream1,
                        s2,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin2,
                        operatorIndex2);

        SingleOutputStreamOperator<DataTuple> joinedStream3 =
                executeJoin(
                        joinedStream2,
                        s3,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin3,
                        operatorIndex3);
        return executeJoin(
                joinedStream3,
                s4,
                dt0 -> getKey(joinBasedClass, dt0),
                dt1 -> getKey(joinBasedClass, dt1),
                windowOperator,
                windowedJoin4,
                operatorIndex4);
    }

    protected SingleOutputStreamOperator<DataTuple> sixWayJoin(
            DataStream<DataTuple> s0,
            DataStream<DataTuple> s1,
            DataStream<DataTuple> s2,
            DataStream<DataTuple> s3,
            DataStream<DataTuple> s4,
            DataStream<DataTuple> s5,
            String h0,
            String h1,
            String h2,
            String h3,
            String h4,
            String h5,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator) {
        String operatorIndex1 = getOperatorIndex(true);
        String operatorIndex2 = getOperatorIndex(true);
        String operatorIndex3 = getOperatorIndex(true);
        String operatorIndex4 = getOperatorIndex(true);
        String operatorIndex5 = getOperatorIndex(true);
        if (windowOperator == null) {
            windowOperator =
                    (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                            operatorProvider.provideOperator(
                                    Constants.Operators.WINDOW, operatorIndex1);
        }

        Class joinBasedClass = RanGen.randClass();
        // collect descriptions from map and window operator
        // windowedJoinDescription.put(Constants.Features.joinKeyClass.name(),
        // mapToPairOperator.getDescription().get(Constants.Features.mapKey.name()));
        HashMap<String, Object> windowedJoinDescription =
                new HashMap<>(windowOperator.getDescription());

        // create join operator
        WindowedJoinOperator windowedJoin1 =
                new WindowedJoinOperator(operatorIndex1, windowedJoinDescription);
        WindowedJoinOperator windowedJoin2 =
                new WindowedJoinOperator(operatorIndex2, windowedJoinDescription);
        WindowedJoinOperator windowedJoin3 =
                new WindowedJoinOperator(operatorIndex3, windowedJoinDescription);
        WindowedJoinOperator windowedJoin4 =
                new WindowedJoinOperator(operatorIndex4, windowedJoinDescription);
        WindowedJoinOperator windowedJoin5 =
                new WindowedJoinOperator(operatorIndex5, windowedJoinDescription);
        windowedJoin1.setJoinKeyClass(joinBasedClass);
        windowedJoin2.setJoinKeyClass(joinBasedClass);
        windowedJoin3.setJoinKeyClass(joinBasedClass);
        windowedJoin4.setJoinKeyClass(joinBasedClass);
        windowedJoin5.setJoinKeyClass(joinBasedClass);

        // build graph
        addQueryVertex(windowedJoin1);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h0),
                currentGraph.getNode(windowedJoin1.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h1),
                currentGraph.getNode(windowedJoin1.getId()));
        curGraphHead = windowedJoin1;

        addQueryVertex(windowedJoin2);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin1.getId()),
                currentGraph.getNode(windowedJoin2.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h2),
                currentGraph.getNode(windowedJoin2.getId()));
        curGraphHead = windowedJoin2;

        addQueryVertex(windowedJoin3);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin2.getId()),
                currentGraph.getNode(windowedJoin3.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h3),
                currentGraph.getNode(windowedJoin3.getId()));
        curGraphHead = windowedJoin3;

        addQueryVertex(windowedJoin4);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin3.getId()),
                currentGraph.getNode(windowedJoin4.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h4),
                currentGraph.getNode(windowedJoin4.getId()));
        curGraphHead = windowedJoin4;

        addQueryVertex(windowedJoin5);
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(windowedJoin4.getId()),
                currentGraph.getNode(windowedJoin5.getId()));
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(h5),
                currentGraph.getNode(windowedJoin5.getId()));
        curGraphHead = windowedJoin5;

        SingleOutputStreamOperator<DataTuple> joinedStream1 =
                executeJoin(
                        s0,
                        s1,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin1,
                        operatorIndex1);

        SingleOutputStreamOperator<DataTuple> joinedStream2 =
                executeJoin(
                        joinedStream1,
                        s2,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin2,
                        operatorIndex2);

        SingleOutputStreamOperator<DataTuple> joinedStream3 =
                executeJoin(
                        joinedStream2,
                        s3,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin3,
                        operatorIndex3);

        SingleOutputStreamOperator<DataTuple> joinedStream4 =
                executeJoin(
                        joinedStream3,
                        s4,
                        dt0 -> getKey(joinBasedClass, dt0),
                        dt1 -> getKey(joinBasedClass, dt1),
                        windowOperator,
                        windowedJoin4,
                        operatorIndex4);
        return executeJoin(
                joinedStream4,
                s5,
                dt0 -> getKey(joinBasedClass, dt0),
                dt1 -> getKey(joinBasedClass, dt1),
                windowOperator,
                windowedJoin5,
                operatorIndex5);
    }

    public <KEY> SingleOutputStreamOperator<DataTuple> executeJoin(
            DataStream<DataTuple> s1,
            DataStream<DataTuple> s2,
            KeySelector<DataTuple, KEY> where,
            KeySelector<DataTuple, KEY> equalTo,
            WindowOperator<? extends WindowAssigner<Object, ? extends Window>> windowOperator,
            WindowedJoinOperator windowedJoinOperator,
            String operatorIndex) {
        if (windowOperator.getEvictor() == null) {
            return ((SingleOutputStreamOperator<DataTuple>)
                            s1.join(s2)
                                    .where(where)
                                    .equalTo(equalTo)
                                    .window(windowOperator.getFunction())
                                    .apply(
                                            windowedJoinOperator.getFunction(),
                                            windowedJoinOperator.getDescription()))
                    .name(operatorIndex);
        } else {
            return ((SingleOutputStreamOperator<DataTuple>)
                            s1.join(s2)
                                    .where(where)
                                    .equalTo(equalTo)
                                    .window(windowOperator.getFunction())
                                    .evictor(windowOperator.getEvictor())
                                    .apply(
                                            windowedJoinOperator.getFunction(),
                                            windowedJoinOperator.getDescription()))
                    .name(operatorIndex);
        }
    }

    protected SingleOutputStreamOperator<DataTuple> applySyntheticWindowedAggregation(
            SingleOutputStreamOperator<DataTuple> stream, boolean keyBy) {
        return applySyntheticWindowedAggregation(stream, keyBy, null, false);
    }

    protected SingleOutputStreamOperator<DataTuple> applySyntheticWindowedAggregation(
            SingleOutputStreamOperator<DataTuple> stream, boolean keyBy, boolean deterministic) {
        return applySyntheticWindowedAggregation(stream, keyBy, null, deterministic);
    }

    protected SingleOutputStreamOperator<DataTuple> applySyntheticWindowedAggregation(
            SingleOutputStreamOperator<DataTuple> stream,
            boolean keyBy,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator) {
        return applySyntheticWindowedAggregation(stream, keyBy, windowOperator, false);
    }

    protected SingleOutputStreamOperator<DataTuple> applySyntheticWindowedAggregation(
            SingleOutputStreamOperator<DataTuple> stream,
            boolean keyBy,
            WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator,
            boolean deterministic) {
        String operatorIndex = getOperatorIndex(true);
        if (windowOperator == null) {
            windowOperator =
                    (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                            operatorProvider.provideOperator(
                                    Constants.Operators.WINDOW, null, deterministic);
        }
        AggregateOperator aggOperator =
                (AggregateOperator)
                        operatorProvider.provideOperator(
                                Constants.Operators.AGGREGATE, operatorIndex, deterministic);
        aggOperator.addWindowDescription(windowOperator.getDescription());
        if (keyBy) {
            Class<?> keyByClass;
            if (deterministic) {
                keyByClass = Constants.Synthetic.Train.DeterministicParameter.KEY_BY_CLASS;
            } else {
                keyByClass = RanGen.randClass();
            }
            aggOperator.setKeyByClass(keyByClass.getSimpleName());
            addQueryVertex(aggOperator);
            addQueryEdge(curGraphHead, aggOperator);
            curGraphHead = aggOperator;
            return executeWindowAggregate(
                    stream,
                    dataTuple -> getKey(keyByClass, dataTuple),
                    windowOperator,
                    aggOperator,
                    operatorIndex);
        } else {
            aggOperator.setKeyByClass("Timestamp");
            addQueryVertex(aggOperator);
            addQueryEdge(curGraphHead, aggOperator);
            curGraphHead = aggOperator;
            KeySelector<DataTuple, Object> keySelector =
                    tuple -> tuple.getTimestamp().substring(tuple.getTimestamp().length() - 2);
            return executeWindowAggregate(
                    stream, keySelector, windowOperator, aggOperator, operatorIndex);
        }
    }

    public SingleOutputStreamOperator<DataTuple> executeWindowAggregate(
            SingleOutputStreamOperator<DataTuple> stream,
            KeySelector<DataTuple, Object> keySelector,
            WindowOperator<? extends WindowAssigner<Object, ? extends Window>> windowOperator,
            AggregateOperator aggregateOperator,
            String operatorIndex) {
        if (keySelector != null) {
            if (windowOperator.getEvictor() == null) {
                return stream.keyBy(keySelector)
                        .window(windowOperator.getFunction())
                        .aggregate(
                                aggregateOperator.getFunction(), aggregateOperator.getDescription())
                        .name(operatorIndex);
            } else {
                return stream.keyBy(keySelector)
                        .window(windowOperator.getFunction())
                        .evictor(windowOperator.getEvictor())
                        .aggregate(
                                aggregateOperator.getFunction(), aggregateOperator.getDescription())
                        .name(operatorIndex);
            }
        } else {
            if (windowOperator.getEvictor() == null) {
                return stream.windowAll(windowOperator.getFunction())
                        .aggregate(
                                aggregateOperator.getFunction(), aggregateOperator.getDescription())
                        .name(operatorIndex);
            } else {
                return stream.windowAll(windowOperator.getFunction())
                        .evictor(windowOperator.getEvictor())
                        .aggregate(
                                aggregateOperator.getFunction(), aggregateOperator.getDescription())
                        .name(operatorIndex);
            }
        }
    }

    /**
     * This adds an query operator with its description to the graph object.
     *
     * @param <T>
     * @param operator The operator to add
     */
    protected <T> void addQueryVertex(AbstractOperator<T> operator) {
        Node n = currentGraph.addNode(operator.getId());
        n.setAttributes(operator.getDescription());
    }

    /**
     * This adds an query edge with its description to the graph object.
     *
     * @param source The source operator
     * @param target The target operator
     */
    protected void addQueryEdge(AbstractOperator source, AbstractOperator target) {
        currentGraph.addEdge(
                nextEdgeIndex(),
                currentGraph.getNode(source.getId()),
                currentGraph.getNode(target.getId()));
    }

    /**
     * Gives the current edge index with the query name and increases it.
     *
     * @return edge index
     */
    protected String nextEdgeIndex() {
        return (currentQueryName + "-" + currentEdgeIndex++);
    }

    /**
     * Gives the current operator index with the query name and increases it.
     *
     * @return operator index
     */
    protected String getOperatorIndex(boolean increment) {
        if (increment) {
            return (currentQueryName + "-" + currentOperatorIndex++);
        } else {
            return (currentQueryName + "-" + currentOperatorIndex);
        }
    }

    protected <U, R> R applyRandomOperator(String type, U stream) {
        return applyRandomOperator(type, stream, false);
    }

    protected <U, R> R applyRandomOperator(String type, U stream, Boolean deterministic) {
        String operatorIndex = getOperatorIndex(true);
        AbstractOperator<?> operator =
                operatorProvider.provideOperator(type, operatorIndex, deterministic);
        addQueryVertex(operator);
        addQueryEdge(curGraphHead, operator);
        curGraphHead = operator;
        // ToDo: currently only used for filter. Maybe find there another solution.
        switch (type) {
            case Constants.Operators.FILTER:
                return (R)
                        ((DataStream<DataTuple>) stream)
                                .filter(
                                        (FilterOperator<DataTuple>) operator,
                                        ((FilterOperator<DataTuple>) operator).getDescription())
                                .name(operatorIndex)
                                .uid(operatorIndex);

            default:
                throw new IllegalArgumentException("Operator not correctly specified");
        }
    }
}
