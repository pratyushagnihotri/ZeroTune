package plangeneratorflink.querybuilder.advertisement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.graphstream.graph.implementations.SingleGraph;
import plangeneratorflink.operators.aggregate.AggregateOperator;
import plangeneratorflink.operators.aggregate.functions.AdvertisementAggregateMeanFunction;
import plangeneratorflink.operators.sink.SinkOperator;
import plangeneratorflink.operators.window.TumblingCountWindows;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

import java.net.UnknownHostException;
import java.util.HashMap;

/** represents the Advertisement Query. */
public class AdvertisementQueryBuilder extends AbstractQueryBuilder {
    @Override
    public SingleGraph buildQuery(
            StreamExecutionEnvironment env, Configuration config, String template, int throughput, String suffix)
            throws UnknownHostException {
        resetBuilder(env, config, template, throughput, suffix);
        final int windowLength = 10 * 1000; // 10 seconds
        final int slidingLength = 1 * 1000; // 1 second
        /*
         * Create Source Impressions
         */
        SingleOutputStreamOperator<DataTuple> impressionsStream =
                createAdvertisementSource(throughput, Constants.AD.SourceType.impressions);
        /*
         * Create WindowAggregate for Impressions
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperatorImpressions =
                new WindowOperator<>(
                        "slidingWindow",
                        "duration",
                        SlidingProcessingTimeWindows.of(
                                Time.milliseconds(windowLength), Time.milliseconds(slidingLength)),
                        windowLength,
                        slidingLength);
        // ToDo: should aggFunctionName be 'mean' or 'ad-mean' here?
        AggregateOperator aggregateOperatorImpressions =
                new AggregateOperator(
                        new AdvertisementAggregateMeanFunction<>(Integer.class),
                        "mean",
                        Integer.class);
        aggregateOperatorImpressions.setId(getOperatorIndex(true));
        aggregateOperatorImpressions.addWindowDescription(
                windowOperatorImpressions.getDescription());
        Class<?> keyByClassImpressions = String.class;
        aggregateOperatorImpressions.setKeyByClass(keyByClassImpressions.getSimpleName());
        addQueryVertex(aggregateOperatorImpressions);
        addQueryEdge(curGraphHead, aggregateOperatorImpressions);
        curGraphHead = aggregateOperatorImpressions;
        impressionsStream =
                executeWindowAggregate(
                        impressionsStream,
                        dt -> getKey(keyByClassImpressions, dt),
                        windowOperatorImpressions,
                        aggregateOperatorImpressions,
                        aggregateOperatorImpressions.getId());
        String head0 = curGraphHead.getId();
        /*
         * Create Source Clicks
         */
        SingleOutputStreamOperator<DataTuple> clicksStream =
                createAdvertisementSource(throughput, Constants.AD.SourceType.clicks);
        /*
         * Create WindowAggregate for Clicks
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperatorClicks =
                new WindowOperator<>(
                        "slidingWindow",
                        "duration",
                        SlidingProcessingTimeWindows.of(
                                Time.milliseconds(windowLength), Time.milliseconds(slidingLength)),
                        windowLength,
                        slidingLength);
        // ToDo: should aggFunctionName be 'mean' or 'ad-mean' here?
        AggregateOperator aggregateOperatorClicks =
                new AggregateOperator(
                        new AdvertisementAggregateMeanFunction<>(Integer.class),
                        "mean",
                        Integer.class);
        aggregateOperatorClicks.setId(getOperatorIndex(true));
        aggregateOperatorClicks.addWindowDescription(windowOperatorClicks.getDescription());
        Class<?> keyByClassClicks = String.class;
        aggregateOperatorClicks.setKeyByClass(keyByClassClicks.getSimpleName());
        addQueryVertex(aggregateOperatorClicks);
        addQueryEdge(curGraphHead, aggregateOperatorClicks);
        curGraphHead = aggregateOperatorClicks;
        clicksStream =
                executeWindowAggregate(
                        clicksStream,
                        dt -> getKey(keyByClassClicks, dt),
                        windowOperatorClicks,
                        aggregateOperatorClicks,
                        aggregateOperatorClicks.getId());
        String head1 = curGraphHead.getId();

        /*
         * Create CountingWindow
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperatorCount =
                new WindowOperator<WindowAssigner<Object, ? extends Window>>(
                        "tumblingWindow", "count", TumblingCountWindows.of(5), 5, null);
        HashMap<String, Object> windowedJoinDescription = new HashMap<>();
        Class<?> joinKlass = String.class;
        windowedJoinDescription.put(Constants.Features.joinKeyClass.name(), joinKlass);
        windowedJoinDescription.putAll(windowOperatorCount.getDescription());
        SingleOutputStreamOperator<DataTuple> joinedStream =
                twoWayJoin(
                        impressionsStream,
                        clicksStream,
                        head0,
                        head1,
                        windowOperatorCount,
                        joinKlass);
        joinedStream
                .addSink(new SinkOperator(this.currentQueryName, config).getFunction())
                .name(getOperatorIndex(false));
        return currentGraph;
    }
}
