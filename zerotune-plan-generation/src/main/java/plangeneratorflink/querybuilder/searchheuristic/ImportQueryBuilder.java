package plangeneratorflink.querybuilder.searchheuristic;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSource;
import org.graphstream.stream.file.FileSourceDOT;
import plangeneratorflink.operators.aggregate.AggregateOperator;
import plangeneratorflink.operators.aggregate.AggregateProvider;
import plangeneratorflink.operators.filter.FilterOperator;
import plangeneratorflink.operators.filter.FilterProvider;
import plangeneratorflink.operators.sink.SinkOperator;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.operators.window.WindowProvider;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.utils.DataTuple;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** ImportQueryBuilder creates a Query out of a .graph file. It assumes it's a synthetic query. */
public class ImportQueryBuilder extends AbstractQueryBuilder {

    HashMap<String, SingleOutputStreamOperator<DataTuple>> operatorsCreated = new HashMap<>();

    public static Graph importQuery(String filePath) {
        String[] splitFilePath = filePath.split("/");
        String graphName =
                splitFilePath[splitFilePath.length - 1].substring(
                        0, splitFilePath[splitFilePath.length - 1].lastIndexOf("."));
        Graph g = new SingleGraph(graphName);
        FileSource fs = new FileSourceDOT();
        fs.addSink(g);
        try {
            fs.readAll(filePath);
        } catch (IOException e) {
            throw new RuntimeException(
                    "Cannot import deterministic query file for search heuristic: "
                            + filePath
                            + " --> "
                            + e.getMessage());
        } finally {
            fs.removeSink(g);
        }
        return g;
    }

    // returns left and right upstream streams for joins.
    private Tuple4<
                    SingleOutputStreamOperator<DataTuple>,
                    SingleOutputStreamOperator<DataTuple>,
                    String,
                    String>
            getUpstreamStreams(Node node) {
        Tuple4<
                        SingleOutputStreamOperator<DataTuple>,
                        SingleOutputStreamOperator<DataTuple>,
                        String,
                        String>
                res = new Tuple4<>();
        ArrayList<Node> upstreamOperators = getUpstreamOperators(node);
        res.f0 = operatorsCreated.get(upstreamOperators.get(0).getId());
        res.f1 = operatorsCreated.get(upstreamOperators.get(1).getId());
        res.f2 = res.f0.getName();
        res.f3 = res.f1.getName();
        return res;
    }

    @Override
    public SingleGraph buildQuery(
            StreamExecutionEnvironment env,
            Configuration config,
            String template,
            int queryNumber,
            String suffix)
            throws UnknownHostException {
        String searchHeuristicFilePath =
                config.getString(
                        ConfigOptions.key("searchHeuristicFilePath").stringType().noDefaultValue());
        resetBuilder(env, config, template, queryNumber, suffix);
        // ToDo: change this to adapt based on the model
        config.setBoolean("searchHeuristicNoJoins", false);
        Graph graph = importQuery(searchHeuristicFilePath);
        convertGraphToQuery(graph);
        return currentGraph;
    }

    @Override
    protected void resetBuilder(
            StreamExecutionEnvironment env,
            Configuration config,
            String template,
            int queryNumber,
            String suffix)
            throws UnknownHostException {
        super.resetBuilder(env, config, template, queryNumber, suffix);
        operatorsCreated = new HashMap<>();
    }

    private void convertGraphToQuery(Graph graph) {
        // search sources
        Stream<Node> sourceNodes =
                graph.nodes()
                        .filter(node -> node.getAttribute("operatorType").equals("SourceOperator"));
        ArrayList<Node> nextNodes = new ArrayList<>();
        sourceNodes.sequential().forEachOrdered(nextNodes::add);
        Node lastNode = null;
        while (nextNodes.size() > 0) {
            Node node = nextNodes.get(0);
            nextNodes.remove(node);
            lastNode = node;
            if (!isOperatorAlreadyCreated(node)) {
                try {
                    createOperator(node);
                    nextNodes.addAll(getDownstreamOperators(node));
                } catch (NullPointerException e) {
                    nextNodes.add(node);
                }
            }
        }
        while (getDownstreamOperators(lastNode).size() > 0) {
            ArrayList<Node> downstreamOperators = getDownstreamOperators(lastNode);
            lastNode = downstreamOperators.get(0);
        }
        operatorsCreated
                .get(lastNode.getId())
                .addSink(new SinkOperator(currentQueryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private ArrayList<Node> getDownstreamOperators(Node node) {
        ArrayList<Node> outgoingEdgeNodes = new ArrayList<>();
        Stream<Edge> outgoingEdges =
                node.edges().filter(edge -> node.getId().equals(edge.getNode0().getId()));
        outgoingEdges
                .sequential()
                .forEachOrdered(
                        edges -> {
                            Node outNode = edges.getNode1();
                            if (!outNode.getAttribute("operatorType").equals("PhysicalNode")) {
                                outgoingEdgeNodes.add(edges.getNode1());
                            }
                        });
        return outgoingEdgeNodes;
    }

    private SingleOutputStreamOperator<DataTuple> createOperator(Node node) {
        SingleOutputStreamOperator<DataTuple> operator = null;
        switch (((String) node.getAttribute("operatorType"))) {
            case "SourceOperator":
                operator = createSourceOperator(node);
                break;
            case "FilterOperator":
                operator = createFilterOperator(node);
                break;
            case "WindowedAggregateOperator":
                operator = createWindowedAggregateOperator(node);
                break;
            case "WindowedJoinOperator":
                config.setBoolean("searchHeuristicNoJoins", false);
                operator = createWindowedJoinOperator(node);
                break;
            default:
                System.out.println("not created a operator for " + node.getId());
                break;
        }
        if (operator != null) {
            operatorsCreated.put(node.getId(), operator);
        }
        return operator;
    }

    private SingleOutputStreamOperator<DataTuple> createWindowedJoinOperator(Node node) {
        // windowJoin has: slidingLength, windowPolicy, joinKeyClass, windowType, windowLength
        Integer slidingLength =
                Integer.parseInt(String.valueOf(node.getAttribute("slidingLength")));
        Integer windowLength = Integer.parseInt(String.valueOf(node.getAttribute("windowLength")));
        String windowPolicy = String.valueOf(node.getAttribute("windowPolicy"));
        String windowType = String.valueOf(node.getAttribute("windowType"));
        String joinKeyClassString = String.valueOf(node.getAttribute("joinKeyClass"));

        Class<?> joinKeyClass = null;
        switch (joinKeyClassString) {
            case "Integer":
                joinKeyClass = Integer.class;
                break;
            case "Double":
                joinKeyClass = Double.class;
                break;
            case "String":
                joinKeyClass = String.class;
                break;
            default:
                throw new RuntimeException("Cannot assign correct keyByClass in searchHeuristic");
        }

        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                createWindowOperator(windowPolicy, windowType, windowLength, slidingLength);
        Tuple4<
                        SingleOutputStreamOperator<DataTuple>,
                        SingleOutputStreamOperator<DataTuple>,
                        String,
                        String>
                upstreamStreams = getUpstreamStreams(node);
        return twoWayJoin(
                upstreamStreams.f0,
                upstreamStreams.f1,
                upstreamStreams.f2,
                upstreamStreams.f3,
                windowOperator,
                joinKeyClass,
                false);
    }

    private SingleOutputStreamOperator<DataTuple> createWindowedAggregateOperator(Node node) {
        // windowAggregate has: slidingLength, windowPolicy, aggClass, windowType, windowLength,
        // keyByClass, aggFunction
        Integer slidingLength =
                Integer.parseInt(String.valueOf(node.getAttribute("slidingLength")));
        Integer windowLength = Integer.parseInt(String.valueOf(node.getAttribute("windowLength")));
        String windowPolicy = (String.valueOf(node.getAttribute("windowPolicy")));
        String aggClass = (String.valueOf(node.getAttribute("aggClass")));
        String windowType = (String.valueOf(node.getAttribute("windowType")));
        String keyByClassString = (String.valueOf(node.getAttribute("keyByClass")));
        String aggFunction = (String.valueOf(node.getAttribute("aggFunction")));

        String operatorIndex = getOperatorIndex(true);
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                createWindowOperator(windowPolicy, windowType, windowLength, slidingLength);
        windowOperator.setId(operatorIndex);

        AggregateOperator aggOperator = createAggregateOperator(aggClass, aggFunction);
        aggOperator.setId(operatorIndex);
        aggOperator.addWindowDescription(windowOperator.getDescription());
        if (!keyByClassString.equals("Timestamp")) {
            Class<?> keyByClass = null;
            switch (keyByClassString) {
                case "Integer":
                    keyByClass = Integer.class;
                    break;
                case "Double":
                    keyByClass = Double.class;
                    break;
                case "String":
                    keyByClass = String.class;
                    break;
                default:
                    throw new RuntimeException(
                            "Cannot assign correct keyByClass in searchHeuristic");
            }
            aggOperator.setKeyByClass(keyByClass.getSimpleName());
            addQueryVertex(aggOperator);
            addQueryEdge(curGraphHead, aggOperator);
            curGraphHead = aggOperator;
            Class<?> finalKeyByClass = keyByClass;
            return executeWindowAggregate(
                    getUpstreamStream(node),
                    dataTuple -> getKey(finalKeyByClass, dataTuple),
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
                    getUpstreamStream(node),
                    keySelector,
                    windowOperator,
                    aggOperator,
                    operatorIndex);
        }
    }

    private AggregateOperator createAggregateOperator(String aggClass, String aggFunction) {
        AggregateProvider aggregateProvider = new AggregateProvider();
        return aggregateProvider.getAggregateFunction(aggFunction, aggClass);
    }

    private WindowOperator<WindowAssigner<Object, ? extends Window>> createWindowOperator(
            String windowPolicy, String windowType, Integer windowLength, Integer slidingLength) {
        WindowProvider windowProvider = new WindowProvider();
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator = null;
        switch (windowPolicy) {
            case "duration":
                switch (windowType) {
                    case "tumblingWindow":
                        return windowProvider.getTumblingTimeWindowOperator(
                                Time.milliseconds(windowLength));
                    case "slidingWindow":
                        return windowProvider.getSlidingTimeWindowOperator(
                                Time.milliseconds(windowLength), Time.milliseconds(slidingLength));
                }
                break;
            case "count":
                switch (windowType) {
                    case "tumblingWindow":
                        return windowProvider.getTumblingCountWindowOperator(windowLength);
                    case "slidingWindow":
                        return windowProvider.getSlidingCountWindowOperator(
                                windowLength, slidingLength);
                }
                break;
        }
        throw new RuntimeException(
                "Cannot create suitable window in SearchHeuristic windowAggregate");
    }

    private SingleOutputStreamOperator<DataTuple> createSourceOperator(Node node) {
        // source has: numDouble, numInteger, numString,confEventRate
        Integer confEventRate =
                (int) Double.parseDouble(String.valueOf(node.getAttribute("confEventRate")));
        Integer numInteger =
                (int) Double.parseDouble(String.valueOf(node.getAttribute("numInteger")));
        Integer numDouble =
                (int) Double.parseDouble(String.valueOf(node.getAttribute("numDouble")));
        Integer numString =
                (int) Double.parseDouble(String.valueOf(node.getAttribute("numString")));
        return createSyntheticSource(confEventRate, numInteger, numDouble, numString);
    }

    private SingleOutputStreamOperator<DataTuple> createFilterOperator(Node node) {
        // filter has: filterClass, filterFunction, literal
        String filterClass = (String.valueOf(node.getAttribute("filterClass")));
        String filterFunction = String.valueOf(node.getAttribute("filterFunction"));
        String literal = String.valueOf(node.getAttribute("literal"));

        FilterProvider filterProvider = new FilterProvider();
        ArrayList<FilterOperator<?>> filterOperators = filterProvider.getOperators();
        Stream<FilterOperator<?>> filterOperatorStream =
                filterOperators.stream()
                        .filter(
                                filterOperator ->
                                        filterOperator
                                                        .getFilterKlass()
                                                        .getSimpleName()
                                                        .equals(filterClass)
                                                && filterOperator
                                                        .getFilterType()
                                                        .equals(filterFunction));
        FilterOperator<?> filterOperator = filterOperatorStream.collect(Collectors.toList()).get(0);
        switch (filterClass) {
            case "Integer":
                filterOperator.setLiteral(Integer.parseInt(literal));
                break;
            case "Double":
                filterOperator.setLiteral(Double.parseDouble(literal));
                break;
            case "String":
                filterOperator.setLiteral(literal);
                break;
        }

        String operatorIndex = getOperatorIndex(true);
        filterOperator.setId(operatorIndex);
        addQueryVertex(filterOperator);
        addQueryEdge(curGraphHead, filterOperator);
        curGraphHead = filterOperator;
        return getUpstreamStream(node)
                .filter(filterOperator, filterOperator.getDescription())
                .name(operatorIndex)
                .uid(operatorIndex);
    }

    // this method assumes that there is only 1 upstream operator. This will not work for joins
    private SingleOutputStreamOperator<DataTuple> getUpstreamStream(Node node) {
        return operatorsCreated.get(getUpstreamOperators(node).get(0).getId());
    }

    private ArrayList<Node> getUpstreamOperators(Node node) {
        ArrayList<Node> incomingEdgeNodes = new ArrayList<>();
        Stream<Edge> incomingEdges =
                node.edges().filter(edge -> node.getId().equals(edge.getNode1().getId()));
        incomingEdges
                .sequential()
                .forEachOrdered(
                        edges -> {
                            Node inNode = edges.getNode0();
                            if (!inNode.getAttribute("operatorType").equals("PhysicalNode")) {
                                incomingEdgeNodes.add(edges.getNode0());
                            }
                        });
        return incomingEdgeNodes;
    }

    private boolean isOperatorAlreadyCreated(Node node) {
        return operatorsCreated.containsKey(node.getId());
    }
}
