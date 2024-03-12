package plangeneratorflink.querybuilder.spikedetection;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.graphstream.graph.implementations.SingleGraph;
import plangeneratorflink.operators.aggregate.AggregateOperator;
import plangeneratorflink.operators.aggregate.functions.AggregateMeanFunction;
import plangeneratorflink.operators.filter.FilterOperator;
import plangeneratorflink.operators.sink.SinkOperator;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.utils.DataTuple;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.function.BiFunction;

/**
 * Detects spikes in values emitted from sensors. Original SpikeDetection example from
 * http://github.com/surajwaghulde/storm-example-projects
 */
public class SpikeDetectionQueryBuilder extends AbstractQueryBuilder {

    @Override
    public SingleGraph buildQuery(
            StreamExecutionEnvironment env,
            Configuration config,
            String template,
            int throughput,
            String suffix)
            throws UnknownHostException {
        resetBuilder(env, config, template, throughput, suffix);
        boolean isRandom =
                true; // differences: isRandom uses Strings for deviceId and Date, isRandom uses
        // values temp values between 50 and 60
        if (throughput == 0) {
            isRandom = false;
        }
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream;
        if (isRandom) {
            stream = createRandomSpikeDetectionSource(throughput);
        } else {
            // ToDo: as we do not use event time, maybe it would make sense to set the timestamp to
            // the processing time timestamp instead of the event timestamp
            stream = createFileSpikeDetectionSource();
        }

        WindowOperator<TumblingProcessingTimeWindows> windowOperator =
                new WindowOperator<>(
                        "tumblingWindow",
                        "duration",
                        TumblingProcessingTimeWindows.of(Time.milliseconds(2000)),
                        2000,
                        null);
        AggregateOperator aggregateOperator =
                new AggregateOperator(
                        new AggregateMeanFunction<>(Double.class), "mean", Double.class);
        aggregateOperator.setId(getOperatorIndex(true));
        aggregateOperator.addWindowDescription(windowOperator.getDescription());
        Class<?> keyByClass;
        if (isRandom) {
            keyByClass = String.class;
        } else {
            keyByClass = Integer.class;
        }
        aggregateOperator.setKeyByClass(keyByClass.getSimpleName());
        addQueryVertex(aggregateOperator);
        addQueryEdge(curGraphHead, aggregateOperator);
        curGraphHead = aggregateOperator;
        stream =
                executeWindowAggregate(
                        stream,
                        dt -> getKey(keyByClass, dt),
                        windowOperator,
                        aggregateOperator,
                        aggregateOperator.getId());
        FilterOperator<Double> filterOperator =
                new FilterOperator<>(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x > y,
                        "greaterThan",
                        Double.class);
        if (isRandom) {
            filterOperator.setLiteral(55.0);
        } else {
            // average temp in spikedetectiondata.txt overall is 39.23514202716347. so we take this
            // as filter criteria
            filterOperator.setLiteral(39.23514202716347);
        }

        filterOperator.setFieldNumber(0);
        filterOperator.setId(getOperatorIndex(true));
        addQueryVertex(filterOperator);
        addQueryEdge(curGraphHead, filterOperator);
        curGraphHead = filterOperator;

        stream =
                stream.filter(filterOperator, filterOperator.getDescription())
                        .name(filterOperator.getId());
        // ToDo: should a sink have a parallelism too?
        stream.addSink(new SinkOperator(this.currentQueryName, config).getFunction())
                .name(getOperatorIndex(false));
        return currentGraph;
    }
}
