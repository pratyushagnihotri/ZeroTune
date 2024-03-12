package plangeneratorflink.querybuilder.synthetic;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.graphstream.graph.implementations.SingleGraph;
import plangeneratorflink.operators.sink.SinkOperator;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

import java.net.UnknownHostException;

import static plangeneratorflink.utils.RanGen.randomBoolean;

/** Generates the training queries. */
public class SyntheticTrainingQueryBuilder extends AbstractQueryBuilder {

    @Override
    public SingleGraph buildQuery(
            StreamExecutionEnvironment env, Configuration config, String template, int queryNumber, String suffix)
            throws UnknownHostException {
        resetBuilder(env, config, template, queryNumber, suffix);
        Boolean useAllOps =
                config.getBoolean(ConfigOptions.key("useAllOps").booleanType().defaultValue(false));
        Boolean deterministic =
                config.getBoolean(
                        ConfigOptions.key("deterministic").booleanType().defaultValue(false));
        if (deterministic) {
            useAllOps = true;
        }
        switch (template) {
            case Constants.Synthetic.Train.TEMPLATE1:
                build_template_1(currentQueryName, useAllOps, deterministic);
                break;
            case Constants.Synthetic.Train.TEMPLATE2:
                build_template_2(currentQueryName, useAllOps, deterministic);
                break;
            case Constants.Synthetic.Train.TEMPLATE3:
                build_template_3(currentQueryName, useAllOps, deterministic);
                break;
            default:
                throw new IllegalArgumentException("Query not implemented");
        }
        // perform duplicate check for synthetic queries
        if (deterministic || checkForDuplicates()) {
            return currentGraph;
        } else {
            return null;
        }
    }

    private void build_template_1(String queryName, Boolean useAllOps, Boolean deterministic) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource(deterministic);

        /*
         * Drop the dice
         */
        boolean filterAtTheStart = randomBoolean();
        boolean keyBy = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (useAllOps
                || System.getProperty("debugMode").equals("true")
                || queryName.matches(".*-0")) {
            filterAtTheStart = true;
            keyBy = true;
            filterAtTheEnd = true;
        }

        /*
         * Generate Query
         */
        if (filterAtTheStart) {
            stream = applyRandomOperator(Constants.Operators.FILTER, stream, deterministic);
        }
        stream = applySyntheticWindowedAggregation(stream, keyBy, deterministic);
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            stream = applyRandomOperator(Constants.Operators.FILTER, stream, deterministic);
        }
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void build_template_2(String queryName, Boolean useAllOps, Boolean deterministic) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (useAllOps
                || System.getProperty("debugMode").equals("true")
                || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource(deterministic);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0, deterministic);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource(deterministic);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1, deterministic);
        }
        String head1 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false), deterministic);
        SingleOutputStreamOperator<DataTuple> joinedStream =
                twoWayJoin(stream0, stream1, head0, head1, windowOperator, null, deterministic);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream =
                applySyntheticWindowedAggregation(
                        joinedStream, keyByStream, windowOperator, deterministic);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream =
                    applyRandomOperator(Constants.Operators.FILTER, joinedStream, deterministic);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void build_template_3(String queryName, Boolean useAllOps, Boolean deterministic) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean filterAtTheStartStream2 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (useAllOps
                || System.getProperty("debugMode").equals("true")
                || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            filterAtTheStartStream2 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource(deterministic);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0, deterministic);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource(deterministic);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1, deterministic);
        }
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource(deterministic);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream2) {
            stream2 = applyRandomOperator(Constants.Operators.FILTER, stream2, deterministic);
        }
        String head2 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false), deterministic);
        SingleOutputStreamOperator<DataTuple> joinedStream =
                threeWayJoin(
                        stream0,
                        stream1,
                        stream2,
                        head0,
                        head1,
                        head2,
                        windowOperator,
                        deterministic);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream =
                applySyntheticWindowedAggregation(
                        joinedStream, keyByStream, windowOperator, deterministic);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream =
                    applyRandomOperator(Constants.Operators.FILTER, joinedStream, deterministic);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }
}
