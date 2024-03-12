package plangeneratorflink.querybuilder.smartgrid;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.graphstream.graph.implementations.SingleGraph;
import plangeneratorflink.operators.aggregate.AggregateOperator;
import plangeneratorflink.operators.aggregate.functions.AggregateMeanFunction;
import plangeneratorflink.operators.sink.SinkOperator;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.utils.DataTuple;

import java.net.UnknownHostException;

public class SmartGridQueryBuilder extends AbstractQueryBuilder {
    @Override
    public SingleGraph buildQuery(
            StreamExecutionEnvironment env,
            Configuration config,
            String template,
            int throughput,
            String suffix)
            throws UnknownHostException {
        resetBuilder(env, config, template, throughput, suffix);
        if (template.contains("global")) {
            return buildGlobalQuery(throughput);
        } else {
            return buildLocalQuery(throughput);
        }
    }

    private SingleGraph buildGlobalQuery(int throughput) {
        int windowLength = 60 * 60 * 1000; // 60 min;
        int slidingLength = 60 * 1000; // 1 min;
        // TODO: following lengths are for debugging purposes, longer lengths can lead to problems
        windowLength = 10 * 1000; // 10s
        slidingLength = 3 * 1000; // 3s
        /*
         * Smart Grid Global
         */
        SingleOutputStreamOperator<DataTuple> stream = createSmartGridSource(throughput);

        WindowOperator<SlidingProcessingTimeWindows> windowOperator =
                new WindowOperator<>(
                        "slidingWindow",
                        "duration",
                        SlidingProcessingTimeWindows.of(
                                Time.milliseconds(windowLength), Time.milliseconds(slidingLength)),
                        windowLength,
                        slidingLength);
        AggregateOperator aggregateOperator =
                new AggregateOperator(
                        new AggregateMeanFunction<>(Double.class), "mean", Double.class);
        aggregateOperator.setId(getOperatorIndex(true));
        aggregateOperator.setKeyByClass("Timestamp");
        aggregateOperator.addWindowDescription(windowOperator.getDescription());
        addQueryVertex(aggregateOperator);
        addQueryEdge(curGraphHead, aggregateOperator);
        curGraphHead = aggregateOperator;
        KeySelector<DataTuple, Object> keySelector =
                tuple -> tuple.getTimestamp().substring(tuple.getTimestamp().length() - 2);
        stream =
                executeWindowAggregate(
                        stream,
                        keySelector,
                        windowOperator,
                        aggregateOperator,
                        aggregateOperator.getId());
        stream.addSink(new SinkOperator(this.currentQueryName, config).getFunction())
                .name(getOperatorIndex(false));
        return currentGraph;
    }

    private SingleGraph buildLocalQuery(int throughput) {
        int windowLength = 60 * 60 * 1000; // 60 min;
        int slidingLength = 60 * 1000; // 1 min;
        // TODO: following lengths are for debugging purposes, longer lengths can lead to problems
        windowLength = 10 * 1000; // 10 s
        slidingLength = 3 * 1000; // 3 s
        /*
         * Smart Grid Local
         */
        SingleOutputStreamOperator<DataTuple> stream = createSmartGridSource(throughput);

        WindowOperator<SlidingProcessingTimeWindows> windowOperator =
                new WindowOperator<>(
                        "slidingWindow",
                        "duration",
                        SlidingProcessingTimeWindows.of(
                                Time.milliseconds(windowLength), Time.milliseconds(slidingLength)),
                        windowLength,
                        slidingLength);
        AggregateOperator aggregateOperator =
                new AggregateOperator(
                        new AggregateMeanFunction<>(Double.class), "mean", Double.class);
        aggregateOperator.setId(getOperatorIndex(true));
        aggregateOperator.addWindowDescription(windowOperator.getDescription());
        Class<?> keyByClass = Integer.class;
        aggregateOperator.setKeyByClass(keyByClass.getSimpleName());
        addQueryVertex(aggregateOperator);
        addQueryEdge(curGraphHead, aggregateOperator);
        curGraphHead = aggregateOperator;
        stream =
                executeWindowAggregate(
                        stream,
                        dt -> getKey(keyByClass, dt, 4),
                        windowOperator,
                        aggregateOperator,
                        aggregateOperator.getId());
        stream.addSink(new SinkOperator(this.currentQueryName, config).getFunction())
                .name(getOperatorIndex(false));
        return currentGraph;
    }
}
