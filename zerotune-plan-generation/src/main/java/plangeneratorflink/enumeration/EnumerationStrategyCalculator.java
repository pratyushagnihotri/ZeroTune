package plangeneratorflink.enumeration;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;

import org.graphstream.graph.Graph;

/** Responsible to prepare and calculate the parallelism depending on the desired strategy. * */
public abstract class EnumerationStrategyCalculator {

    protected int minParallelism;
    protected int maxParallelism;
    Configuration config;

    public EnumerationStrategyCalculator(Configuration config) {
        this.config = config;
        this.minParallelism =
                config.getInteger(ConfigOptions.key("minParallelism").intType().noDefaultValue());
        this.maxParallelism =
                config.getInteger(ConfigOptions.key("maxParallelism").intType().noDefaultValue());
    }

    public abstract void initNewQuery(StreamGraph streamGraph, Graph pgfGraph, String runName);

    public abstract <T> Integer getParallelismOfOperator(
            StreamGraph streamGraph, Graph pgfGraph, StreamNode node, String runName);

    public abstract boolean hasNext(String runName);

    public int getNextIndex() {
        throw new IllegalStateException(
                "cannot call getNextIndex() except for search heuristic prediction comparison enumeration strategy.");
    }
}
