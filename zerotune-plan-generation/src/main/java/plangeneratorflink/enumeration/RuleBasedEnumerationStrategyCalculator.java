package plangeneratorflink.enumeration;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import plangeneratorflink.utils.Constants;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicBoolean;

/** Applies the rule based parallelism strategy to operators. * */
public class RuleBasedEnumerationStrategyCalculator extends EnumerationStrategyCalculator {

    private StreamNode node;
    private Node opPGFNode;
    // rule based map containing calculated parallelism, estimated
    // outputRate, estimated selectivity of each operator
    private HashMap<String, RBESEstimator> rbm = new HashMap<>();

    public RuleBasedEnumerationStrategyCalculator(Configuration config) {
        super(config);
    }

    // source parallelism:
    // factor? y=0.02*x^(0,5)

    // other operators:
    // get estimated output rate of upstream operator
    // get processing complexity factor
    // for join consider half of both?

    // estimated event output rate of upstream operator:
    // previously * selectivity

    // ideas:
    // find a "base parallelism", e.g. depending on input event rate and hardware
    // reduce "base parallelism" after windowAggregate (mind the difference between time and
    // cound windows)

    // windowAggregate with keyBy: parallelism should be same as amount of keys?? (is this
    // correct?)
    //

    @Override
    public void initNewQuery(StreamGraph streamGraph, Graph pgfGraph, String runName) {
        rbm = new HashMap<>();
        AtomicBoolean everyOperatorHasAnParallelism = new AtomicBoolean(false);
        while (!everyOperatorHasAnParallelism.get()) {
            everyOperatorHasAnParallelism.set(true);
            pgfGraph.nodes()
                    .forEachOrdered(
                            pgfNode -> {
                                String operatorName = pgfNode.getId();
                                RBESEstimator rbE =
                                        rbm.computeIfAbsent(operatorName, RBESEstimator::new);
                                if (!rbE.isParallelismSet()) {
                                    switch (pgfNode.getAttribute("operatorType", String.class)) {
                                        case "SourceOperator":
                                            calculateSourceOperator(pgfNode, rbE);
                                            break;
                                        case "FilterOperator":
                                            calculateFilterOperator(pgfNode, rbE);
                                            break;
                                        case "WindowedAggregateOperator":
                                            calculateWindowedAggregateOperator(pgfNode, rbE);
                                            break;
                                        case "WindowedJoinOperator":
                                            calculateJoinOperator(pgfNode, rbE);
                                            break;
                                    }
                                    System.out.println(rbE);
                                    if (rbE.isParallelismSet()) {
                                        everyOperatorHasAnParallelism.set(false);
                                    }
                                }
                            });
        }
    }

    private void calculateJoinOperator(Node pgfNode, RBESEstimator rbE) {
        ArrayList<Integer> estimatedUpstreamOutputRates = new ArrayList<>();
        pgfNode.enteringEdges()
                .forEachOrdered(
                        edge -> {
                            // check if edge is really an entering edge (because graph is undirected
                            // at this point)
                            if (!edge.getNode0().getId().equals(pgfNode.getId())) {
                                RBESEstimator upstreamRbE = rbm.get(edge.getNode0().getId());
                                if (upstreamRbE != null) {
                                    estimatedUpstreamOutputRates.add(
                                            upstreamRbE.getEstimatedOutputRate());
                                }
                            }
                        });
        String windowType = pgfNode.getAttribute("windowType", String.class);
        String windowPolicy = pgfNode.getAttribute("windowPolicy", String.class);
        Integer windowLength = pgfNode.getAttribute("windowLength", Integer.class);
        Integer slidingLength = pgfNode.getAttribute("slidingLength", Integer.class);
        String joinClass = pgfNode.getAttribute("joinKeyClass", String.class);
        double estimatedSelectivity =
                calculateEstimatedJoinSelectivity(
                        estimatedUpstreamOutputRates,
                        windowType,
                        windowPolicy,
                        windowLength,
                        slidingLength,
                        joinClass);
        rbE.setEstimatedSelectivity(estimatedSelectivity);
        int summedUpInputRates = estimatedUpstreamOutputRates.stream().mapToInt(i -> i).sum();
        rbE.setEstimatedOutputRate((int) (summedUpInputRates * estimatedSelectivity));
        // join parallelism is calculated by checking estimated input and output rates because they
        // can differentiate a lot in both directions
        rbE.setParallelism(
                Math.max(
                        getEstimatedJoinParallelism(summedUpInputRates),
                        getEstimatedJoinParallelism(rbE.getEstimatedOutputRate())));
    }

    private double calculateEstimatedJoinSelectivity(
            ArrayList<Integer> estimatedInputRates,
            String windowType,
            String windowPolicy,
            Integer windowLength,
            Integer slidingLength,
            String joinClass) {
        // how big is the window?
        // what is the join probability?
        // windowSize * join probability
        double estimatedSelectivity = 0;
        int joinRangeSize = getKeyByRangeSize(joinClass);
        int sumEstimatedInputRates = estimatedInputRates.stream().mapToInt(i -> i).sum();
        switch (windowPolicy) {
            case "duration":
                switch (windowType) {
                    case "tumblingWindow":
                        {
                            // (0.0007*windowLength * 0,0.15 * sumEstimatedInputRates^1/2))/2
                            estimatedSelectivity =
                                    (0.001
                                                    * windowLength
                                                    * 0.015
                                                    * Math.pow(sumEstimatedInputRates, 0.5))
                                            / 2.0;
                        }
                        break;
                    case "slidingWindow":
                        {
                            estimatedSelectivity =
                                    ((0.001
                                                            * windowLength
                                                            * 0.015
                                                            * Math.pow(sumEstimatedInputRates, 0.5))
                                                    / 2.0)
                                            / (1.0 * slidingLength / windowLength);
                        }
                        break;
                }
                break;
            case "count":
                switch (windowType) {
                    case "tumblingWindow":
                        {
                            estimatedSelectivity = 0.189 * windowLength;
                        }
                        break;
                    case "slidingWindow":
                        {
                            estimatedSelectivity =
                                    (0.189 * windowLength) / (1.0 * slidingLength / windowLength);
                        }
                        break;
                }
                break;
        }
        return estimatedSelectivity;
    }

    private void calculateWindowedAggregateOperator(Node pgfNode, RBESEstimator rbE) {
        pgfNode.enteringEdges()
                .forEachOrdered(
                        edge -> {
                            // check if edge is really an entering edge (because graph is undirected
                            // at this point)
                            if (!edge.getNode0().getId().equals(pgfNode.getId())) {
                                RBESEstimator upstreamRbE = rbm.get(edge.getNode0().getId());
                                if (upstreamRbE != null) {
                                    int estimatedUpstreamOutputRate =
                                            upstreamRbE.getEstimatedOutputRate();
                                    rbE.setParallelism(
                                            getEstimatedWindowAggregateParallelism(
                                                    estimatedUpstreamOutputRate));
                                    String windowType =
                                            pgfNode.getAttribute("windowType", String.class);
                                    String windowPolicy =
                                            pgfNode.getAttribute("windowPolicy", String.class);
                                    String keyByClass =
                                            pgfNode.getAttribute("keyByClass", String.class);
                                    Integer windowLength =
                                            pgfNode.getAttribute("windowLength", Integer.class);
                                    Integer slidingLength =
                                            pgfNode.getAttribute("slidingLength", Integer.class);
                                    double estimatedSelectivity =
                                            calculateEstimatedWindowAggregateSelectivity(
                                                    windowType,
                                                    windowPolicy,
                                                    keyByClass,
                                                    windowLength,
                                                    slidingLength,
                                                    estimatedUpstreamOutputRate);
                                    rbE.setEstimatedSelectivity(estimatedSelectivity);
                                    rbE.setEstimatedOutputRate(
                                            (int)
                                                    (estimatedUpstreamOutputRate
                                                            * estimatedSelectivity));
                                }
                            }
                        });
    }

    private double calculateEstimatedWindowAggregateSelectivity(
            String windowType,
            String windowPolicy,
            String keyByClass,
            Integer windowLength,
            Integer slidingLength,
            int estimatedInputRate) {
        int keyRangeSize = getKeyByRangeSize(keyByClass);
        Double estimatedSelectivity = null;
        switch (windowPolicy) {
            case "duration":
                switch (windowType) {
                    case "tumblingWindow":
                        {
                            double inputPerKey = 1.0 * estimatedInputRate / keyRangeSize;
                            double outputPerKey = 1.0 / (windowLength / 1000.0);
                            estimatedSelectivity = outputPerKey / inputPerKey;
                        }
                        break;
                    case "slidingWindow":
                        {
                            double inputPerKey = 1.0 * estimatedInputRate / keyRangeSize;
                            double outputPerKey = 1.0 / (slidingLength / 1000.0);
                            estimatedSelectivity = outputPerKey / inputPerKey;
                        }
                        break;
                }
                break;
            case "count":
                switch (windowType) {
                    case "tumblingWindow":
                        estimatedSelectivity = (1.0 / windowLength);
                        break;
                    case "slidingWindow":
                        estimatedSelectivity = (1.0 / slidingLength);
                        break;
                }
                break;
        }
        if (estimatedSelectivity == null) {
            throw new RuntimeException(
                    "Cannot determine estimated selectivity of "
                            + windowType
                            + " ("
                            + windowPolicy
                            + ", "
                            + keyByClass
                            + ", "
                            + windowLength
                            + ", "
                            + slidingLength
                            + ")");
        }
        return estimatedSelectivity;
    }

    private int getKeyByRangeSize(String keyByClass) {
        int rangeSize;
        switch (keyByClass) {
            case "String":
                // keyBy uses last character of timestamp, so the range is [All_Chars][0-9]
                return Constants.Synthetic.Train.ALL_CHARS.length() * 10;
            case "Integer":
                {
                    Tuple2<Integer, Integer> r = Constants.Synthetic.Train.INTEGER_VALUE_RANGE;
                    TreeSet<Integer> ts = new TreeSet<>();
                    for (int i = r.f0; i <= r.f1; i++) {
                        for (int j = 0; j <= 9; j++) {
                            ts.add(i * j);
                        }
                    }
                    return ts.size();
                }
            case "Double":
                {
                    Tuple2<Double, Double> r = Constants.Synthetic.Train.DOUBLE_VALUE_RANGE;
                    TreeSet<Double> ts = new TreeSet<>();
                    // + 0.005 to exclude rounding inaccuracies
                    for (double i = r.f0; i <= r.f1 + 0.005; i = i + 0.01) {
                        for (double j = 0; j <= 9; j++) {
                            ts.add(i * j);
                        }
                    }
                    return ts.size();
                }
            case "Timestamp":
                return 10 * 10;
        }
        throw new RuntimeException("Cannot determine keyByRangeSize for " + keyByClass);
    }

    private void calculateFilterOperator(Node pgfNode, RBESEstimator rbE) {
        // TODO: Implement data characteristic bias because of filter or windowAggregate beforehand
        pgfNode.enteringEdges()
                .forEachOrdered(
                        edge -> {
                            // check if edge is really an entering edge (because graph is undirected
                            // at this point)
                            if (!edge.getNode0().getId().equals(pgfNode.getId())) {
                                RBESEstimator upstreamRbE = rbm.get(edge.getNode0().getId());
                                if (upstreamRbE != null) {
                                    int estimatedUpstreamOutputRate =
                                            upstreamRbE.getEstimatedOutputRate();
                                    rbE.setParallelism(
                                            getEstimatedFilterParallelism(
                                                    estimatedUpstreamOutputRate));
                                    String filterFunction =
                                            pgfNode.getAttribute("filterFunction", String.class);
                                    String filterClass =
                                            pgfNode.getAttribute("filterClass", String.class);
                                    Object filterLiteral = pgfNode.getAttribute("literal");
                                    double estimatedSelectivity =
                                            calculateEstimatedFilterSelectivity(
                                                    filterFunction, filterClass, filterLiteral);
                                    rbE.setEstimatedSelectivity(estimatedSelectivity);
                                    rbE.setEstimatedOutputRate(
                                            (int)
                                                    (estimatedUpstreamOutputRate
                                                            * estimatedSelectivity));
                                }
                            }
                        });
    }

    private Double calculateEstimatedFilterSelectivity(
            String filterFunction, String filterClass, Object filterLiteral) {
        Double estimatedSelectivity = null;
        switch (filterClass) {
            case "String":
                int stringLength = Constants.Synthetic.Train.STRING_LENGTH;
                int charRangeSize = Constants.Synthetic.Train.ALL_CHARS.length();
                int literalLength = ((String) filterLiteral).length();
                switch (filterFunction) {
                    case "startsWith":
                        estimatedSelectivity = 1.0 / (charRangeSize * literalLength);
                        break;
                    case "endsWith":
                        estimatedSelectivity = 1.0 / (charRangeSize * literalLength);
                        break;
                    case "endsNotWith":
                        estimatedSelectivity = 1 - (1.0 / (charRangeSize * literalLength));
                        break;
                    case "startsNotWith":
                        estimatedSelectivity = 1 - (1.0 / (charRangeSize * literalLength));
                        break;
                    case "contains":
                        // 1-(1-(1/charRangeSize)^literalLength)^stringLength-literalLength+1
                        estimatedSelectivity =
                                1
                                        - Math.pow(
                                                1 - Math.pow((1.0 / charRangeSize), literalLength),
                                                stringLength - literalLength + 1);
                        break;
                }
                break;
            case "Integer":
                int filterLiteralInt = ((Integer) filterLiteral);
                Tuple2<Integer, Integer> integerValueRange =
                        Constants.Synthetic.Train.INTEGER_VALUE_RANGE;
                int integerValueRangeSize = integerValueRange.f1 - integerValueRange.f0;
                switch (filterFunction) {
                    case "notEquals":
                        estimatedSelectivity = 1 - (1.0 / integerValueRangeSize);
                        break;
                    case "greaterThan":
                        // (valueRangeMax - filterLiteral) / rangeSize
                        estimatedSelectivity =
                                ((double) integerValueRange.f1 - filterLiteralInt)
                                        / integerValueRangeSize;
                        break;
                    case "lessThan":
                        // (filterLiteral - valueRangeMin) / rangeSize
                        estimatedSelectivity =
                                ((double) filterLiteralInt - integerValueRange.f0)
                                        / integerValueRangeSize;
                        break;
                    case "greaterEquals":
                        // (valueRangeMax - filterLiteral + 1) / rangeSize
                        estimatedSelectivity =
                                ((double) integerValueRange.f1 - filterLiteralInt + 1)
                                        / integerValueRangeSize;
                        break;
                    case "lessEquals":
                        // (filterLiteral - valueRangeMin) / rangeSize
                        estimatedSelectivity =
                                ((double) filterLiteralInt - integerValueRange.f0 + 1)
                                        / integerValueRangeSize;
                        break;
                }
                break;
            case "Double":
                double filterLiteralDouble = ((Double) filterLiteral);
                Tuple2<Double, Double> doubleValueRange =
                        Constants.Synthetic.Train.DOUBLE_VALUE_RANGE;
                double doubleValueRangeSize =
                        (doubleValueRange.f1 - doubleValueRange.f0) * 100
                                + 1; // * 100 because we round by 2 digits and
                // therefore diff between 0 and 1 contains
                // 100 different values
                switch (filterFunction) {
                    case "notEquals":
                        estimatedSelectivity = 1.0 - (1.0 / doubleValueRangeSize);
                        break;
                    case "greaterThan":
                        estimatedSelectivity =
                                ((doubleValueRange.f1 - filterLiteralDouble) * 100)
                                        / doubleValueRangeSize;
                        break;
                    case "lessThan":
                        // (filterLiteral - valueRangeMin) / rangeSize
                        estimatedSelectivity =
                                ((filterLiteralDouble - doubleValueRange.f0) * 100)
                                        / doubleValueRangeSize;
                        break;
                    case "greaterEquals":
                        // (valueRangeMax - filterLiteral + 1) / rangeSize
                        estimatedSelectivity =
                                ((doubleValueRange.f1 - filterLiteralDouble) * 100 + 1)
                                        / doubleValueRangeSize;
                        break;
                    case "lessEquals":
                        // (filterLiteral - valueRangeMin) / rangeSize
                        estimatedSelectivity =
                                ((filterLiteralDouble - doubleValueRange.f0) * 100 + 1)
                                        / doubleValueRangeSize;
                        break;
                }
                break;
        }
        if (estimatedSelectivity == null) {
            throw new RuntimeException(
                    "Cannot determine estimated selectivity of "
                            + filterFunction
                            + " ("
                            + filterClass
                            + ")");
        }
        return estimatedSelectivity;
    }

    private void calculateSourceOperator(Node pgfNode, RBESEstimator rbE) {
        int eventRate = pgfNode.getAttribute("confEventRate", Integer.class);
        rbE.setParallelism(getEstimatedSourceParallelism(eventRate));
        // ToDo: Hardware aspects could influence estimated output rate
        // / selectivity
        rbE.setEstimatedSelectivity(1);
        rbE.setEstimatedOutputRate(eventRate);
    }

    // TODO: consider hardware, tuple widths, other factors into account in this formula
    // parallelism = (0,03*expectedEventRate)^(1 /
    // 2.15)+(5E-11^expectedEventRate)^2-(5E-18*expectedEventRate)^3
    private int getEstimatedSourceParallelism(int expectedEventRate) {
        return (int) (0.000006 * Math.pow(expectedEventRate, 1.17));
    }

    private int getEstimatedFilterParallelism(int estimatedInputRate) {
        return (int) (0.000006 * Math.pow(estimatedInputRate, 1.17));
    }

    private int getEstimatedWindowAggregateParallelism(int estimatedInputRate) {
        return (int) (0.000007 * Math.pow(estimatedInputRate, 1.17));
    }

    private int getEstimatedJoinParallelism(int sumEstimatedInputRates) {
        return (int) (0.000006 * Math.pow(sumEstimatedInputRates, 1.17));
    }

    @Override
    public <T> Integer getParallelismOfOperator(
            StreamGraph streamGraph, Graph pgfGraph, StreamNode node, String runName) {
        this.node = node;
        String operatorName =
                node.getOperatorName().replaceFirst("^Source: ", "").replaceFirst("^Sink: ", "");
        this.opPGFNode = pgfGraph.getNode(operatorName);
        if (opPGFNode != null) {
            return validate(rbm.get(operatorName).getParallelism());
        } else {
            System.out.println("op is not part of the graph node. Todo");
            return 1;
        }
    }

    @Override
    public boolean hasNext(String runName) {
        return true;
    }

    /**
     * This method validates the given parallelism is in its boundaries and restrict it if needed to
     * minParallelism and maxParallelism. *
     */
    private int validate(int parallelism) {
        return Math.min(Math.max(parallelism, minParallelism), maxParallelism);
    }
}
