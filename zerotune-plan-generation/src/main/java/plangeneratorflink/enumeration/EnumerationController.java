package plangeneratorflink.enumeration;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;

import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import plangeneratorflink.utils.Constants;

import java.util.List;

/**
 * Makes the decision which degree of parallelism to apply for a certain operator based on the
 * parallelism strategy. For further information, the ParallelismController may use the previous
 * QueryGraph or previously generated queries. With the ParallelismStrategy Random, the degree of
 * parallelism is chosen randomly. With Exhaustive, every possible combination of the degree of
 * parallelism is set for each query. With rule-based, a degree of parallelism is defined based on a
 * rule system and the previous operators. Greedy is not defined yet.
 */
public class EnumerationController {
    private final Configuration config;
    private final Constants.EnumerationStrategy strategy;
    private EnumerationStrategyCalculator esc;

    public EnumerationController(Configuration config, Constants.EnumerationStrategy strategy) {
        this.config = config;
        this.strategy = strategy;
        reset();
    }

    public void reset() {
        if (strategy == null) {
            throw new IllegalArgumentException("ParallelismStrategy cannot be null.");
        }
        int numTopos = config.getInteger(ConfigOptions.key("numTopos").intType().noDefaultValue());
        int stepSize = config.getInteger(ConfigOptions.key("stepSize").intType().defaultValue(1));
        switch (strategy) {
            case RANDOM:
                this.esc = new RandomEnumerationStrategyCalculator(config);
                return;
            case RULEBASED:
                this.esc = new RuleBasedEnumerationStrategyCalculator(config);
                return;
            case SHPREDICTIVECOMPARISON:
                this.esc = new SHPredictiveComparison(config);
                return;
            default:
                throw new IllegalArgumentException("strategy not implemented yet.");
        }
    }

    public boolean hasNext(String runName) {
        return this.esc.hasNext(runName);
    }

    public void setParallelismForStreamGraph(
            StreamGraph streamGraph, Graph pgfGraph, String runName) {
        this.esc.initNewQuery(streamGraph, pgfGraph, runName);
        for (StreamNode node : streamGraph.getStreamNodes()) {
            StreamOperator<?> op = node.getOperator();
            // only set parallelism for operators from graph
            String operatorName =
                    node.getOperatorName()
                            .replaceFirst("^Source: ", "")
                            .replaceFirst("^Sink: ", "");
            Node pgfGraphNode = pgfGraph.getNode(operatorName);
            if (pgfGraphNode != null) {
                int parallelism =
                        this.esc.getParallelismOfOperator(streamGraph, pgfGraph, node, runName);
                node.setParallelism(parallelism);
                streamGraph.setMaxParallelism(node.getId(), parallelism);
                pgfGraphNode.setAttribute(Constants.Features.parallelism.name(), parallelism);
            }
        }
        // if the operator is a join operator, set the map() operators of every input to
        // the same parallelism as the operator before that map() to ensure chaining
        // (intimidate flinks default behavior)
        for (StreamNode node : streamGraph.getStreamNodes()) {
            String operatorName =
                    node.getOperatorName()
                            .replaceFirst("^Source: ", "")
                            .replaceFirst("^Sink: ", "");
            Node pgfGraphNode = pgfGraph.getNode(operatorName);
            if (pgfGraphNode != null) {
                if (pgfGraphNode
                        .getAttribute("operatorType", String.class)
                        .equals("WindowedJoinOperator")) {
                    for (StreamEdge inEdge : node.getInEdges()) {
                        StreamNode mapNode = streamGraph.getStreamNode(inEdge.getSourceId());
                        int parallelismOfInputOperator = 0;
                        for (StreamEdge inEdgeOfMap : mapNode.getInEdges()) {
                            parallelismOfInputOperator =
                                    streamGraph
                                            .getStreamNode(inEdgeOfMap.getSourceId())
                                            .getParallelism();
                        }
                        mapNode.setParallelism(parallelismOfInputOperator);
                    }
                }
            }
        }
        // set partitioner after all parallelisms are set
        for (StreamNode node : streamGraph.getStreamNodes()) {
            setPartitioner(streamGraph, node);
        }
    }

    private void setPartitioner(StreamGraph streamGraph, StreamNode node) {
        List<StreamEdge> inEdges = node.getInEdges();
        for (StreamEdge streamEdge : inEdges) {
            StreamNode upstreamNode = streamGraph.getStreamNode(streamEdge.getSourceId());
            StreamPartitioner<?> partitioner = streamEdge.getPartitioner();
            if (partitioner instanceof ForwardPartitioner
                    || partitioner instanceof RebalancePartitioner) {
                boolean sameParallelism = upstreamNode.getParallelism() == node.getParallelism();
                streamEdge.setPartitioner(
                        sameParallelism
                                ? new ForwardPartitioner<>()
                                : new RebalancePartitioner<>());
            }
        }
    }

    public int getNextIndex() {
        return esc.getNextIndex();
    }
}
