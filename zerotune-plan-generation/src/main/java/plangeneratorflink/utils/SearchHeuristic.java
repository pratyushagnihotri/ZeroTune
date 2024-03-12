package plangeneratorflink.utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.stream.file.FileSinkDOT;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import plangeneratorflink.querybuilder.searchheuristic.ImportQueryBuilder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/** Searches for the optimum parallelism degrees based on predicted and actual costs. * */
// dry parameters are always used even when they are wet, except for predictive comparison, the term
// is a little bit misleading
public class SearchHeuristic {

    private final Configuration config;
    private final String logFolder;
    private boolean searchHeuristicPredictions;

    public SearchHeuristic(
            Configuration config, String logFolder, boolean searchHeuristicPredictions) {
        this.config = config;
        this.logFolder = logFolder;
        this.searchHeuristicPredictions = searchHeuristicPredictions;
        deleteFiles(false);
    }

    private void deleteFiles(boolean onlySearchHeuristics) {
        // delete old log files
        new File(String.valueOf(Paths.get(logFolder, "searchHeuristic.csv"))).delete();
        new File(
                        String.valueOf(
                                Paths.get(
                                        logFolder, "searchHeuristicWithPredictiveComparison.csv")))
                .delete();
        if (onlySearchHeuristics) {
            return;
        }
        new File(String.valueOf(Paths.get(logFolder, "query.labels"))).delete();
        File[] files = new File(String.valueOf(Paths.get(logFolder, "predictions"))).listFiles();
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
        new File(String.valueOf(Paths.get(logFolder, "predictions"))).delete();

        files = new File(String.valueOf(Paths.get(logFolder, "graphs"))).listFiles();
        if (files != null) {
            for (File file : files) {
                file.delete();
            }
        }
        new File(String.valueOf(Paths.get(logFolder, "graphs"))).delete();
    }

    public void createArtificalGraphFile(
            String templateQueryPath,
            String logFolder,
            String queryName,
            HashMap<Integer, Integer> parallelismSet) {
        Graph g = ImportQueryBuilder.importQuery(templateQueryPath);
        for (Node n : g.nodes().collect(Collectors.toList())) {
            if (n.getAttribute("operatorType").equals("PhysicalNode")) {
                continue;
            }
            String id = n.getId();
            String[] idSplitted = id.split("-");
            Integer parallelism =
                    parallelismSet.get(Integer.valueOf(idSplitted[idSplitted.length - 1]));
            n.setAttribute("parallelism", parallelism);
        }
        FileSinkDOT fs = new FileSinkDOT();
        try {
            File directory = new File(logFolder + "/graphs/");
            if (!directory.exists()) {
                directory.mkdir();
            }
            fs.writeAll(g, logFolder + "/graphs/" + queryName + ".graph");
            Files.writeString(
                    Path.of(logFolder + "/query.labels"),
                    "{\"duration\": 30.1, \"throughput\": 1, \"counter\": 1, \"id\": \""
                            + queryName
                            + "\", \"latency\": 1}"
                            + "\n",
                    CREATE,
                    APPEND);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void generatePrediction() throws IOException {
        String trainingDataPath =
                config.getString(
                        ConfigOptions.key("searchHeuristicModelPath")
                                .stringType()
                                .noDefaultValue());
        String searchHeuristicZerotuneLearningPath =
                config.getString(
                        ConfigOptions.key("searchHeuristicZerotuneLearningPath")
                                .stringType()
                                .noDefaultValue());
        boolean noJoins =
                config.getBoolean(
                        ConfigOptions.key("searchHeuristicNoJoins")
                                .booleanType()
                                .defaultValue(false));

        String cleanUpCommand =
                "python3 -c 'import sys;sys.path.append(\""
                        + searchHeuristicZerotuneLearningPath
                        + "\");from learning.preprocessing.dataset_analyzer import analyze_graphs;analyze_graphs(\""
                        + logFolder
                        + "\", False);'";
        ProcessBuilder cpBuilder = new ProcessBuilder("/bin/sh", "-c", cleanUpCommand);
        cpBuilder.redirectErrorStream(true);
        String cleanUpResponse = new String(cpBuilder.start().getInputStream().readAllBytes());
        System.out.println("cleanUpResponse:\n" + cleanUpResponse);

        List<String> metrics = Arrays.asList("latency", "throughput");
        for (String metric : metrics) {
            String predictionCommand =
                    "python3 "
                            + searchHeuristicZerotuneLearningPath
                            + "/main.py"
                            + " --mode "
                            + " test "
                            + " --metric "
                            + metric
                            + " --training_data "
                            + trainingDataPath
                            + " --test_data "
                            + logFolder
                            + (noJoins ? " --no_joins " : "");

            ProcessBuilder pBuilder = new ProcessBuilder("/bin/sh", "-c", predictionCommand);
            pBuilder.redirectErrorStream(true);
            String predictionResponse =
                    new String(pBuilder.start().getInputStream().readAllBytes());
            System.out.println("predictionResponse " + metric + ":\n" + predictionResponse);
        }
    }

    public void determineOptimalCosts() throws IOException {
        DecimalFormat df = new DecimalFormat("0.0000");
        // determine prediction costs
        // get latency costs
        List<List<String>> latencyPredictions = importPredictions("latency");
        List<List<String>> throughputPredictions = importPredictions("throughput");
        HashSet<SearchHeuristicCosts> costs = new HashSet<>();
        generateHashSetOutOfPredictions(latencyPredictions, throughputPredictions, costs);
        calculateOverallCosts(costs, true, false, true);
        if (searchHeuristicPredictions) {
            Files.writeString(
                    Path.of(logFolder + "/searchHeuristic.csv"),
                    "name,predOverallCosts,predLatency,predThroughput,parallelismSet" + "\n",
                    CREATE,
                    APPEND);
        } else {
            Files.writeString(
                    Path.of(logFolder + "/searchHeuristic.csv"),
                    "name,predOverallCosts,predLatency,predThroughput,actOverallCosts,actLatency,actThroughput,qErrorLatency,qErrorThroughput,parallelismSet"
                            + "\n",
                    CREATE,
                    APPEND);
        }
        // output predicted costs
        System.out.println("========= Predicted Costs =========");
        List<SearchHeuristicCosts> orderedPredResults =
                costs.stream()
                        .sorted(Comparator.comparing(SearchHeuristicCosts::getDryPredOverallCosts))
                        .collect(Collectors.toList());
        for (SearchHeuristicCosts v : orderedPredResults) {
            if (searchHeuristicPredictions) {
                String outputString =
                        v.getQueryId()
                                + ","
                                + v.getDryPredOverallCosts()
                                + ","
                                + v.getDryPredLatency()
                                + ","
                                + v.getDryPredTp()
                                + ","
                                + v.getFormattedParallelismSet();
                Files.writeString(
                        Path.of(logFolder + "/searchHeuristic.csv"),
                        outputString + "\n",
                        CREATE,
                        APPEND);
            }
            System.out.println(
                    v.getQueryId()
                            + " --> "
                            + df.format(v.getDryPredOverallCosts())
                            + " (latency: "
                            + df.format(v.getDryPredLatency())
                            + " | throughput: "
                            + df.format(v.getDryPredTp())
                            + ")"
                            + " => "
                            + v.getFormattedParallelismSet());
        }
        // output actual costs
        if (!searchHeuristicPredictions) {
            System.out.println("\n========= Actual Costs =========");
            List<SearchHeuristicCosts> orderedActualResults =
                    costs.stream()
                            .sorted(Comparator.comparing(SearchHeuristicCosts::getActOverallCosts))
                            .collect(Collectors.toList());
            for (SearchHeuristicCosts v : orderedActualResults) {
                String outputString =
                        v.getQueryId()
                                + ","
                                + v.getDryPredOverallCosts()
                                + ","
                                + v.getDryPredLatency()
                                + ","
                                + v.getDryPredTp()
                                + ","
                                + v.getActOverallCosts()
                                + ","
                                + v.getActLatency()
                                + ","
                                + v.getActTp()
                                + ","
                                + v.getDryQErrorLatency()
                                + ","
                                + v.getDryQErrorTp()
                                + ","
                                + v.getFormattedParallelismSet();
                Files.writeString(
                        Path.of(logFolder + "/searchHeuristic.csv"),
                        outputString + "\n",
                        CREATE,
                        APPEND);
                System.out.println(
                        v.getQueryId()
                                + " --> "
                                + df.format(v.getActOverallCosts())
                                + " (latency: "
                                + df.format(v.getActLatency())
                                + " | throughput: "
                                + df.format(v.getActTp())
                                + ")"
                                + " => "
                                + v.getFormattedParallelismSet());
            }
        }
    }

    public void generatePredictiveComparisonCSVFile() throws IOException, ParseException {
        //delete existing searchHeuristic to generate a new one which also contains both, the dryRun predictions and the wetRun predictions
        deleteFiles(true);
        searchHeuristicPredictions = false;
        generatePrediction();
        determineOptimalCosts();
        // read searchHeuristic.csv
        List<List<String>> searchHeuristicRecords = new ArrayList<>();
        String logFolder =
                config.getString(ConfigOptions.key("logDir").stringType().defaultValue(""));
        BufferedReader br =
                new BufferedReader(
                        new FileReader(
                                String.valueOf(Paths.get(logFolder, "searchHeuristic.csv"))));
        String line;
        br.readLine(); // skip header
        while ((line = br.readLine()) != null) {
            String[] values = line.split(",");
            searchHeuristicRecords.add(Arrays.asList(values));
        }
        // read query.labels
        JSONParser parser = new JSONParser();
        ArrayList<JSONObject> labels = new ArrayList<>();

        BufferedReader reader =
                new BufferedReader(
                        new FileReader(String.valueOf(Paths.get(logFolder, "query.labels"))));

        while ((line = reader.readLine()) != null) {
            Object obj = parser.parse(line);
            labels.add((JSONObject) obj);
        }
        // combine dry run searchHeuristic with wet run searchHeuristic
        Files.writeString(
                Path.of(logFolder + "/searchHeuristicWithPredictiveComparison.csv"),
                "name,dryPredOverallCosts,dryPredLatency,dryPredThroughput,wetPredOverallCosts,wetPredLatency,wetPredThroughput,actOverallCosts,actLatency,actThroughput,dryQErrorLatency,dryQErrorThroughput,wetQErrorLatency,wetQErrorThroughput,parallelismSet"
                        + "\n",
                CREATE,
                APPEND);
        HashSet<SearchHeuristicCosts> searchHeuristicCosts = new HashSet<>();
        for (List<String> record : searchHeuristicRecords) {
            String queryName = record.get(0);
            // skip if its not an artificial query
            if (!queryName.contains("searchHeuristicArtificial")) {
                continue;
            }
            int queryIndex = Integer.parseInt(queryName.substring(queryName.lastIndexOf("-") + 1));
            SearchHeuristicCosts costs = new SearchHeuristicCosts(queryName);
            List<List<String>> predictionComparisonList =
                    searchHeuristicRecords.stream()
                            .filter(
                                    r ->
                                            r.get(0)
                                                    .endsWith(
                                                            "searchHeuristicPredictionComparison-"
                                                                    + queryIndex))
                            .collect(Collectors.toList());
            List<String> predictionComparison = null;
            if (predictionComparisonList.size() > 0) {
                predictionComparison = predictionComparisonList.get(0);
            } else {
                // cannot find query, maybe it was excluded for actual running or has been excluded
                // in the dataset analyzer.
                continue;
            }
            try {
                costs.setDryPredLatency(Double.valueOf(record.get(2)));
                costs.setDryPredTp(Double.valueOf(record.get(3)));
                costs.setParallelismSet(getParallelismSet(queryName));
                costs.setWetPredLatency(Double.valueOf(predictionComparison.get(2)));
                costs.setWetPredTp(Double.valueOf(predictionComparison.get(3)));
                JSONObject label =
                        labels.stream()
                                .filter(
                                        r ->
                                                ((String) r.get("id"))
                                                        .endsWith(
                                                                "searchHeuristicPredictionComparison-"
                                                                        + queryIndex))
                                .collect(Collectors.toList())
                                .get(0);
                costs.setActLatency(Double.valueOf(String.valueOf(label.get("latency"))));
                costs.setActTp(Double.valueOf(String.valueOf(label.get("throughput"))));
                costs.setDryQErrorLatency(getQError(costs, true, true));
                costs.setDryQErrorTp(getQError(costs, true, false));
                costs.setWetQErrorLatency(getQError(costs, false, true));
                costs.setWetQErrorTp(getQError(costs, false, false));
                searchHeuristicCosts.add(costs);
            } catch (Exception e) {
                System.err.println(e);
            }
        }
        calculateOverallCosts(searchHeuristicCosts, true, true, true);

        List<SearchHeuristicCosts> orderedPredResults =
                searchHeuristicCosts.stream()
                        .sorted(Comparator.comparing(SearchHeuristicCosts::getActOverallCosts))
                        .collect(Collectors.toList());
        for (SearchHeuristicCosts v : orderedPredResults) {
            StringBuilder sb = new StringBuilder();
            sb.append(v.getQueryId()).append(",");
            sb.append(v.getDryPredOverallCosts()).append(",");
            sb.append(v.getDryPredLatency()).append(",");
            sb.append(v.getDryPredTp()).append(",");
            sb.append(v.getWetPredOverallCosts()).append(",");
            sb.append(v.getWetPredLatency()).append(",");
            sb.append(v.getWetPredTp()).append(",");
            sb.append(v.getActOverallCosts()).append(",");
            sb.append(v.getActLatency()).append(",");
            sb.append(v.getActTp()).append(",");
            sb.append(v.getDryQErrorLatency()).append(",");
            sb.append(v.getDryQErrorTp()).append(",");
            sb.append(v.getWetQErrorLatency()).append(",");
            sb.append(v.getWetQErrorTp()).append(",");
            sb.append(v.getFormattedParallelismSet()).append(",");

            Files.writeString(
                    Path.of(logFolder + "/searchHeuristicWithPredictiveComparison.csv"),
                    sb.toString() + "\n",
                    CREATE,
                    APPEND);
        }
    }

    private double getQError(SearchHeuristicCosts costs, boolean useDry, boolean useLatency) {
        double pred;
        double act;

        if (useDry) {
            if (useLatency) {
                pred = costs.getDryPredLatency();
                act = costs.getActLatency();
            } else {
                pred = costs.getDryPredTp();
                act = costs.getActTp();
            }
        } else {
            if (useLatency) {
                pred = costs.getWetPredLatency();
                act = costs.getActLatency();
            } else {
                pred = costs.getWetPredTp();
                act = costs.getActTp();
            }
        }

        pred = Math.abs(pred);
        act = Math.abs(act);
        return Math.max((act / pred), (pred / act));
    }

    private HashMap<String, Tuple2<String, Integer>> getParallelismSet(String queryId) {
        Graph g = ImportQueryBuilder.importQuery(logFolder + "/graphs/" + queryId + ".graph");
        List<Node> nodes =
                g.nodes()
                        .filter(node -> !node.getAttribute("operatorType").equals("PhysicalNode"))
                        .collect(Collectors.toList());
        HashMap<String, Tuple2<String, Integer>> operatorParallelism = new HashMap<>();
        for (Node node : nodes) {
            operatorParallelism.put(
                    node.getId(),
                    new Tuple2<>(
                            (String) node.getAttribute("operatorType"),
                            ((Double) node.getAttribute("parallelism")).intValue()));
        }
        return operatorParallelism;
    }

    private void calculateOverallCosts(
            HashSet<SearchHeuristicCosts> costs,
            boolean calculateDryPredictionCosts,
            boolean calculateWetPredictionCosts,
            boolean calculateActualCosts) {
        double latencyPortion = 0.5;

        //
        // Dry Predictions
        //
        if (calculateDryPredictionCosts) {
            double latencyPredMin = Double.MAX_VALUE;
            double latencyPredMax = 0.0;
            double tpPredMin = Double.MAX_VALUE;
            double tpPredMax = 0.0;
            // get min & max values for scaling
            for (SearchHeuristicCosts costEntry : costs) {
                if (costEntry.getDryPredLatency() < latencyPredMin) {
                    latencyPredMin = costEntry.getDryPredLatency();
                }
                if (costEntry.getDryPredLatency() > latencyPredMax) {
                    latencyPredMax = costEntry.getDryPredLatency();
                }
                if (costEntry.getDryPredTp() < tpPredMin) {
                    tpPredMin = costEntry.getDryPredTp();
                }
                if (costEntry.getDryPredTp() > tpPredMax) {
                    tpPredMax = costEntry.getDryPredTp();
                }
            }

            // generate costs
            for (SearchHeuristicCosts costEntry : costs) {
                // calculate predicted overall costs
                double predictedLatency =
                        normalizeCosts(
                                costEntry.getDryPredLatency(),
                                latencyPredMin,
                                latencyPredMax,
                                false);
                double predictedThroughput =
                        normalizeCosts(costEntry.getDryPredTp(), tpPredMin, tpPredMax, true);
                costEntry.setDryPredOverallCosts(
                        predictedLatency * latencyPortion
                                + predictedThroughput * (1 - latencyPortion));
            }
        }

        if (calculateWetPredictionCosts) {
            double latencyWetPredMin = Double.MAX_VALUE;
            double latencyWetPredMax = 0.0;
            double tpWetPredMin = Double.MAX_VALUE;
            double tpWetPredMax = 0.0;
            // get min & max values for scaling
            for (SearchHeuristicCosts costEntry : costs) {
                if (costEntry.getWetPredLatency() < latencyWetPredMin) {
                    latencyWetPredMin = costEntry.getWetPredLatency();
                }
                if (costEntry.getWetPredLatency() > latencyWetPredMax) {
                    latencyWetPredMax = costEntry.getWetPredLatency();
                }
                if (costEntry.getWetPredTp() < tpWetPredMin) {
                    tpWetPredMin = costEntry.getWetPredTp();
                }
                if (costEntry.getWetPredTp() > tpWetPredMax) {
                    tpWetPredMax = costEntry.getWetPredTp();
                }
            }

            // generate costs
            for (SearchHeuristicCosts costEntry : costs) {
                // calculate predicted overall costs
                double predictedWetLatency =
                        normalizeCosts(
                                costEntry.getWetPredLatency(),
                                latencyWetPredMin,
                                latencyWetPredMax,
                                false);
                double predictedWetThroughput =
                        normalizeCosts(costEntry.getWetPredTp(), tpWetPredMin, tpWetPredMax, true);
                costEntry.setWetPredOverallCosts(
                        predictedWetLatency * latencyPortion
                                + predictedWetThroughput * (1 - latencyPortion));
            }
        }

        if (calculateActualCosts) {
            double latencyActualMin = Double.MAX_VALUE;
            double latencyActualMax = 0.0;
            double tpActualMin = Double.MAX_VALUE;
            double tpActualMax = 0.0;
            // get min & max values for scaling
            for (SearchHeuristicCosts costEntry : costs) {
                if (costEntry.getActLatency() < latencyActualMin) {
                    latencyActualMin = costEntry.getActLatency();
                }
                if (costEntry.getActLatency() > latencyActualMax) {
                    latencyActualMax = costEntry.getActLatency();
                }
                if (costEntry.getActTp() < tpActualMin) {
                    tpActualMin = costEntry.getActTp();
                }
                if (costEntry.getActTp() > tpActualMax) {
                    tpActualMax = costEntry.getActTp();
                }
            }

            // generate costs
            for (SearchHeuristicCosts costEntry : costs) {
                // calculate actual overall costs
                double actualLatency =
                        normalizeCosts(
                                costEntry.getActLatency(),
                                latencyActualMin,
                                latencyActualMax,
                                false);
                double actualThroughput =
                        normalizeCosts(costEntry.getActTp(), tpActualMin, tpActualMax, true);
                costEntry.setActOverallCosts(
                        actualLatency * latencyPortion + actualThroughput * (1 - latencyPortion));
            }
        }
    }

    private double normalizeCosts(double value, double min, double max, boolean invert) {
        if (max == min) {
            // if we haven't a range to scale (-> only 1 queryrun?) we scale from 0
            min = 0;
        }
        double v = (value - min) / (max - min);
        return invert ? 1 - v : v;
    }

    private void generateHashSetOutOfPredictions(
            List<List<String>> latencyPredictions,
            List<List<String>> throughputPredictions,
            HashSet<SearchHeuristicCosts> costs) {
        for (List<String> prediction : latencyPredictions) {
            String queryId = prediction.get(0);
            List<SearchHeuristicCosts> existingSHC =
                    costs.stream()
                            .filter(c -> c.getQueryId().equals(queryId))
                            .collect(Collectors.toList());
            SearchHeuristicCosts queryCostEntry =
                    existingSHC.size() > 0 ? existingSHC.get(0) : new SearchHeuristicCosts(queryId);
            costs.add(queryCostEntry);
            queryCostEntry.setDryPredLatency(Double.parseDouble(prediction.get(2)));
            queryCostEntry.setActLatency(Double.parseDouble(prediction.get(1)));
            queryCostEntry.setDryQErrorLatency(Double.parseDouble(prediction.get(3)));
            if (queryCostEntry.getParallelismSet() == null) {
                try {
                    queryCostEntry.setParallelismSet(getParallelismSet(queryId));
                } catch (Exception e) {
                    continue;
                }
            }
        }
        for (List<String> prediction : throughputPredictions) {
            String queryId = prediction.get(0);
            List<SearchHeuristicCosts> existingSHC =
                    costs.stream()
                            .filter(c -> c.getQueryId().equals(queryId))
                            .collect(Collectors.toList());
            SearchHeuristicCosts queryCostEntry =
                    existingSHC.size() > 0 ? existingSHC.get(0) : new SearchHeuristicCosts(queryId);
            costs.add(queryCostEntry);
            queryCostEntry.setDryPredTp(Double.parseDouble(prediction.get(2)));
            queryCostEntry.setActTp(Double.parseDouble(prediction.get(1)));
            queryCostEntry.setDryQErrorTp(Double.parseDouble(prediction.get(3)));
            if (queryCostEntry.getParallelismSet() == null) {
                try {
                    queryCostEntry.setParallelismSet(getParallelismSet(queryId));
                } catch (Exception e) {
                    continue;
                }
            }
        }
    }

    private List<List<String>> importPredictions(String metric) {
        List<List<String>> records = new ArrayList<>();
        try (BufferedReader br =
                new BufferedReader(
                        new FileReader(logFolder + "/predictions/model-" + metric + ".pred.csv"))) {
            String line;
            br.readLine(); // skip header
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                records.add(Arrays.asList(values));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return records;
    }

    public HashMap<Integer, Integer> getParallelismSet(StreamGraph artificialStreamGraph) {
        HashMap<Integer, Integer> parallelismSet =
                new HashMap<>(); // key: operator number, value: parallelism
        for (StreamNode streamNode : artificialStreamGraph.getStreamNodes()) {
            String operatorName = streamNode.getOperatorName();
            if (operatorName.equals("Map")) {
                continue;
            }
            String[] operatorNameSplitted = operatorName.split("-");
            int parallelism = streamNode.getParallelism();
            parallelismSet.put(
                    Integer.valueOf(operatorNameSplitted[operatorNameSplitted.length - 1]),
                    parallelism);
        }
        return parallelismSet;
    }
}
