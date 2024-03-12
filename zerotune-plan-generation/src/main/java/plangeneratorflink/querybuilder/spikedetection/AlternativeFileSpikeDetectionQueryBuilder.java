package plangeneratorflink.querybuilder.spikedetection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.graphstream.graph.implementations.SingleGraph;
import plangeneratorflink.operators.aggregate.AggregateOperator;
import plangeneratorflink.operators.aggregate.functions.AggregateSpikeDetectionMeanFunction;
import plangeneratorflink.operators.filter.FilterOperator;
import plangeneratorflink.operators.sink.SinkOperator;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.utils.DataTuple;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.function.BiFunction;

// this is not needed anymore, maybe later for an additional evaluation of applying the model to
// unseen operators

/**
 * Detects spikes in values emitted from sensors.
 * http://github.com/surajwaghulde/storm-example-projects Author surajwaghulde
 */
public class AlternativeFileSpikeDetectionQueryBuilder extends AbstractQueryBuilder {

    @Override
    public SingleGraph buildQuery(
            StreamExecutionEnvironment env, Configuration config, String template, int throughput, String suffix)
            throws UnknownHostException {
        resetBuilder(env, config, template, throughput, suffix);
        // ToDo: use event time or processing time?
        // ToDo: define on which field the spikedetection should happen?
        // ToDo: is the window correct?
        // ToDo: check output graph
        // ToDo: maybe add attributes to Constants?
        // ToDo: is spikedetection and keyBy on correct fields?
        // ToDo: where / How to save data file?
        // ToDo: Implement AggregateSpikeDetectionMeanFunction
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createFileSpikeDetectionSource();
        //        /*
        //         * Parse to DataTuple
        //         */
        //        FlatMapOperator<String> flatMapOperator = new
        // FlatMapOperator<>(getOperatorIndex(true), FileSpikeDetectionQueryBuilder::flatMap);
        //        SingleOutputStreamOperator<DataTuple> streamDataTuple =
        // stream.flatMap(flatMapOperator.getFunction()).returns(DataTuple.class).name(flatMapOperator.getId());
        //        streamDataTuple.print();
        /*
         * Calculate Average
         */
        String operatorIndex = getOperatorIndex(true);
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                new WindowOperator<>(
                        "tumblingWindow",
                        "duration",
                        TumblingProcessingTimeWindows.of(Time.milliseconds(2000)),
                        2000,
                        null);
        //ToDo: should it be meanspikedetection or mean?
        AggregateOperator aggOperator =
                new AggregateOperator(
                        new AggregateSpikeDetectionMeanFunction<>(Double.class),
                        "mean",
                        Double.class);
        aggOperator.setId(operatorIndex);
        curGraphHead = aggOperator;
        aggOperator.addWindowDescription(windowOperator.getDescription());
        addQueryVertex(aggOperator);
        addQueryEdge(curGraphHead, aggOperator);
        aggOperator.setKeyByClass(Integer.class.getSimpleName());
        stream =
                executeWindowAggregate(
                        stream,
                        tuple -> getKey(Integer.class, tuple, 1),
                        windowOperator,
                        aggOperator,
                        operatorIndex);
        /*
         * Perform Spike Detection
         */
        FilterOperator<Double> filterOperator =
                new FilterOperator<>(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x > y,
                        "greaterThan",
                        Double.class);
        filterOperator.setId(getOperatorIndex(true));
        addQueryVertex(filterOperator);
        addQueryEdge(curGraphHead, filterOperator);
        curGraphHead = filterOperator;

        stream =
                stream.filter(filterOperator, filterOperator.getDescription())
                        .name(filterOperator.getId());
        stream.addSink(new SinkOperator(currentQueryName, config).getFunction())
                .name(getOperatorIndex(false));

        return currentGraph;
    }
}
