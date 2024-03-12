package plangeneratorflink.enumeration;

import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;

import org.graphstream.graph.Graph;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Only to be used in combination with Search Heuristic. Runs Querys with parallelism sets out of
 * the searchHeuristic.csv file
 */
public class SHPredictiveComparison extends EnumerationStrategyCalculator {

    HashMap<String, HashMap<Integer, Integer>> parallelismSets;
    ArrayList<String> queryNames;
    HashMap<Integer, Integer> currentParallelismSet;
    int pickSize = 100;

    public SHPredictiveComparison(Configuration config) {
        super(config);
        // read searchHeuristic file
        List<List<String>> searchHeuristic = importSearchHeuristic();
        // extract parallelism sets we want to run and prepare list
        searchHeuristic = pick(searchHeuristic);
        parallelismSets = extractParallelismSets(searchHeuristic);
        queryNames = new ArrayList<>(parallelismSets.keySet());
    }

    // picks first x, middle x and end x amount of queries to run, depending on pickSize
    private List<List<String>> pick(List<List<String>> searchHeuristic) {
        int size = searchHeuristic.size();
        List<List<String>> newSearchHeuristic = new ArrayList<>();
        if (size > 3 * pickSize) {
            int startTop = 0;
            int endTop = pickSize - 1;
            int startMid = (size / 2) - (pickSize / 2);
            int endMid = (size / 2) + (pickSize / 2);
            int startBottom = size - pickSize;
            int endBottom = size - 1;

            for (int i = 0; i < searchHeuristic.size(); i++) {
                if ((i >= startTop && i <= endTop)
                        || (i >= startMid && i <= endMid)
                        || (i >= startBottom && i <= endBottom)) {
                    newSearchHeuristic.add(searchHeuristic.get(i));
                }
            }
            return newSearchHeuristic;
        }
        return searchHeuristic;
    }

    private HashMap<String, HashMap<Integer, Integer>> extractParallelismSets(
            List<List<String>> searchHeuristic) {
        HashMap<String, HashMap<Integer, Integer>> parallelismSets = new HashMap<>();
        // pattern to parse parallelism set combinations from searchHeuristic.csv
        Pattern pattern =
                Pattern.compile(
                        "\\((?<OperatorType>\\D)(?<OperatorID>\\d+)->P(?<Parallelism>\\d+)\\)");
        // for every searchHeuristic.csv line
        for (List<String> ps : searchHeuristic) {
            HashMap<Integer, Integer> map = new HashMap<>();
            // 4th attribute contains parallelism sets
            String s = ps.get(4);
            String[] split = s.split(" ");
            for (String s2 : split) {
                final Matcher matcher = pattern.matcher(s2);
                if (matcher.matches()) {
                    // String operatorType = matcher.group("OperatorType");
                    Integer operatorID = Integer.parseInt(matcher.group("OperatorID"));
                    Integer parallelism = Integer.parseInt(matcher.group("Parallelism"));
                    map.put(operatorID, parallelism);
                }
            }
            // put parallelism set into parallelismSets hashmap
            if (map.size() > 0) {
                parallelismSets.put(ps.get(0), map);
            }
        }
        return parallelismSets;
    }

    @Override
    public void initNewQuery(StreamGraph streamGraph, Graph pgfGraph, String runName) {
        // set new parallelism set
        String queryName = queryNames.get(0);
        queryNames.remove(queryName);
        currentParallelismSet = parallelismSets.get(queryName);
    }

    @Override
    public <T> Integer getParallelismOfOperator(
            StreamGraph streamGraph, Graph pgfGraph, StreamNode node, String runName) {
        StreamOperator<?> op = node.getOperator();
        if (op instanceof StreamSink
                || (op instanceof WindowOperator
                        && ((WindowOperator<?, ?, ?, ?, ?>) op).getKeySelector()
                                instanceof NullByteKeySelector)) {
            return 1;
        }
        String operatorName = node.getOperatorName();
        int opId = Integer.parseInt(operatorName.substring(operatorName.lastIndexOf("-") + 1));
        return currentParallelismSet.get(opId);
    }

    @Override
    public boolean hasNext(String runName) {
        return !queryNames.isEmpty();
    }

    @Override
    public int getNextIndex() {
        String s = queryNames.get(0);
        return Integer.parseInt(s.substring(s.lastIndexOf("-") + 1));
    }

    private List<List<String>> importSearchHeuristic() {
        List<List<String>> records = new ArrayList<>();
        String logFolder =
                config.getString(ConfigOptions.key("logDir").stringType().defaultValue(""));
        try (BufferedReader br =
                new BufferedReader(
                        new FileReader(
                                String.valueOf(Paths.get(logFolder, "searchHeuristic.csv"))))) {
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
}
