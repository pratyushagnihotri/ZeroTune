package plangeneratorflink.querybuilder.synthetic;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.windows.Window;

import org.graphstream.graph.implementations.SingleGraph;
import plangeneratorflink.operators.sink.SinkOperator;
import plangeneratorflink.operators.window.WindowOperator;
import plangeneratorflink.operators.window.WindowProvider;
import plangeneratorflink.querybuilder.AbstractQueryBuilder;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

import java.net.UnknownHostException;

import static plangeneratorflink.utils.RanGen.randomBoolean;

/** Test Queries. */
public class SyntheticTestQueryBuilder extends AbstractQueryBuilder {

    WindowProvider wp = new WindowProvider();

    @Override
    public SingleGraph buildQuery(
            StreamExecutionEnvironment env, Configuration config, String template, int queryNumber, String suffix)
            throws UnknownHostException {
        resetBuilder(env, config, template, queryNumber, suffix);
        String[] templateSplit = template.split("-");
        switch (templateSplit[0]) {
            case Constants.Synthetic.Test.TESTA:
                buildTemplateA(
                        templateSplit[1], Integer.valueOf(templateSplit[3]), currentQueryName);
                break;
            case Constants.Synthetic.Test.TESTB:
                buildTemplateB(
                        templateSplit[1], Integer.valueOf(templateSplit[3]), currentQueryName);
                break;
            case Constants.Synthetic.Test.TESTC:
                buildTemplateC(
                        templateSplit[1], Integer.valueOf(templateSplit[4]), currentQueryName);
                break;
            case Constants.Synthetic.Test.TESTD:
                buildTemplateD(
                        templateSplit[1], Integer.valueOf(templateSplit[4]), currentQueryName);
                break;
            case Constants.Synthetic.Test.TESTE:
                buildTemplateE(templateSplit[1], currentQueryName);
                break;
            default:
                throw new IllegalArgumentException("Query not implemented");
        }
        // perform duplicate check for synthetic queries
        if (checkForDuplicates()) {
            return currentGraph;
        } else {
            return null;
        }
    }

    /** T0: extrapolated tuple widths. */
    private void buildTemplateA(String template, Integer tupleWidth, String queryName) {
        switch (template) {
            case "template1":
                buildTemplateA1(tupleWidth, queryName);
                break;
            case "template2":
                buildTemplateA2(tupleWidth, queryName);
                break;
            case "template3":
                buildTemplateA3(tupleWidth, queryName);
                break;
        }
    }

    private void buildTemplateB(String template, Integer eventRate, String queryName) {
        switch (template) {
            case "template1":
                buildTemplateB1(eventRate, queryName);
                break;
            case "template2":
                buildTemplateB2(eventRate, queryName);
                break;
            case "template3":
                buildTemplateB3(eventRate, queryName);
                break;
        }
    }

    private void buildTemplateC(String template, Integer windowDuration, String queryName) {
        switch (template) {
            case "template1":
                buildTemplateC1(windowDuration, queryName);
                break;
            case "template2":
                buildTemplateC2(windowDuration, queryName);
                break;
            case "template3":
                buildTemplateC3(windowDuration, queryName);
                break;
        }
    }

    private void buildTemplateD(String template, Integer windowLength, String queryName) {
        switch (template) {
            case "template1":
                buildTemplateD1(windowLength, queryName);
                break;
            case "template2":
                buildTemplateD2(windowLength, queryName);
                break;
            case "template3":
                buildTemplateD3(windowLength, queryName);
                break;
        }
    }

    private void buildTemplateE(String template, String queryName) {
        switch (template) {
            case "4wayjoin":
                buildTemplateE4WayJoin(queryName);
                break;
            case "5wayjoin":
                buildTemplateE5WayJoin(queryName);
                break;
            case "6wayjoin":
                buildTemplateE6WayJoin(queryName);
                break;
            case "twoFilters":
                buildTemplateE2Filters(queryName);
                break;
            case "threeFilters":
                buildTemplateE3Filters(queryName);
                break;
            case "fourFilters":
                buildTemplateE4Filters(queryName);
                break;
        }
    }

    private void buildTemplateA1(Integer tupleWidth, String queryName) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource(null, tupleWidth);

        /*
         * Drop the dice
         */
        boolean filterAtTheStart = randomBoolean();
        boolean keyBy = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStart = true;
            keyBy = true;
            filterAtTheEnd = true;
        }

        /*
         * Generate Query
         */
        if (filterAtTheStart) {
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream = applySyntheticWindowedAggregation(stream, keyBy);
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateA2(Integer tupleWidth, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource(null, tupleWidth);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource(null, tupleWidth);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                twoWayJoin(stream0, stream1, head0, head1, windowOperator, null);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateA3(Integer tupleWidth, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean filterAtTheStartStream2 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            filterAtTheStartStream2 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource(null, tupleWidth);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource(null, tupleWidth);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource(null, tupleWidth);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream2) {
            stream2 = applyRandomOperator(Constants.Operators.FILTER, stream2);
        }
        String head2 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                threeWayJoin(stream0, stream1, stream2, head0, head1, head2, windowOperator);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateB1(Integer eventRate, String queryName) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource(eventRate, null);

        /*
         * Drop the dice
         */
        boolean filterAtTheStart = randomBoolean();
        boolean keyBy = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStart = true;
            keyBy = true;
            filterAtTheEnd = true;
        }

        /*
         * Generate Query
         */
        if (filterAtTheStart) {
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream = applySyntheticWindowedAggregation(stream, keyBy);
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateB2(Integer eventRate, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource(eventRate, null);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource(eventRate, null);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                twoWayJoin(stream0, stream1, head0, head1, windowOperator, null);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateB3(Integer eventRate, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean filterAtTheStartStream2 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            filterAtTheStartStream2 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource(eventRate, null);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource(eventRate, null);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource(eventRate, null);

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream2) {
            stream2 = applyRandomOperator(Constants.Operators.FILTER, stream2);
        }
        String head2 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                threeWayJoin(stream0, stream1, stream2, head0, head1, head2, windowOperator);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateC1(Integer windowDuration, String queryName) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource();

        /*
         * Drop the dice
         */
        boolean filterAtTheStart = randomBoolean();
        boolean keyBy = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStart = true;
            keyBy = true;
            filterAtTheEnd = true;
        }

        /*
         * Generate Query
         */
        if (filterAtTheStart) {
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream =
                applySyntheticWindowedAggregation(
                        stream, keyBy, wp.getRandomTimeBasedWindowOp(windowDuration));
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateC2(Integer windowDuration, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                wp.getRandomTimeBasedWindowOp(windowDuration);
        windowOperator.setId(getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                twoWayJoin(stream0, stream1, head0, head1, windowOperator, null);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateC3(Integer windowDuration, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean filterAtTheStartStream2 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            filterAtTheStartStream2 = true;
            keyByStream = true;
            filterAtTheEnd = false;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream2) {
            stream2 = applyRandomOperator(Constants.Operators.FILTER, stream2);
        }
        String head2 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                wp.getRandomTimeBasedWindowOp(windowDuration);
        windowOperator.setId(getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                threeWayJoin(stream0, stream1, stream2, head0, head1, head2, windowOperator);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateD1(Integer windowLength, String queryName) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource();

        /*
         * Drop the dice
         */
        boolean filterAtTheStart = randomBoolean();
        boolean keyBy = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStart = true;
            keyBy = true;
            filterAtTheEnd = true;
        }

        /*
         * Generate Query
         */
        if (filterAtTheStart) {
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream =
                applySyntheticWindowedAggregation(
                        stream, keyBy, wp.getRandomCountBasedWindowOp(windowLength));
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        }
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateD2(Integer windowLength, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                wp.getRandomCountBasedWindowOp(windowLength);
        windowOperator.setId(getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                twoWayJoin(stream0, stream1, head0, head1, windowOperator, null);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateD3(Integer windowLength, String queryName) {
        /*
         * Drop the dice
         */
        boolean filterAtTheStartStream0 = randomBoolean();
        boolean filterAtTheStartStream1 = randomBoolean();
        boolean filterAtTheStartStream2 = randomBoolean();
        boolean keyByStream = randomBoolean();
        boolean filterAtTheEnd = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            filterAtTheStartStream0 = true;
            filterAtTheStartStream1 = true;
            filterAtTheStartStream2 = true;
            keyByStream = true;
            filterAtTheEnd = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream0) {
            stream0 = applyRandomOperator(Constants.Operators.FILTER, stream0);
        }
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream1) {
            stream1 = applyRandomOperator(Constants.Operators.FILTER, stream1);
        }
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource();

        /*
         * Perhaps filter after stream creation
         */
        if (filterAtTheStartStream2) {
            stream2 = applyRandomOperator(Constants.Operators.FILTER, stream2);
        }
        String head2 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                wp.getRandomCountBasedWindowOp(windowLength);
        windowOperator.setId(getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                threeWayJoin(stream0, stream1, stream2, head0, head1, head2, windowOperator);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        /*
         * Perhaps filter and forward to sink
         */
        if (filterAtTheEnd) {
            // ToDo: need filter based on keyBy class?
            joinedStream = applyRandomOperator(Constants.Operators.FILTER, joinedStream);
        }
        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateE4WayJoin(String queryName) {
        /*
         * Drop the dice
         */
        boolean keyByStream = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            keyByStream = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource();
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource();
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource();
        String head2 = curGraphHead.getId();
        /*
         * Define Source 3
         */
        SingleOutputStreamOperator<DataTuple> stream3 = createSyntheticSource();
        String head3 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                fourWayJoin(
                        stream0,
                        stream1,
                        stream2,
                        stream3,
                        head0,
                        head1,
                        head2,
                        head3,
                        windowOperator);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateE5WayJoin(String queryName) {
        /*
         * Drop the dice
         */
        boolean keyByStream = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            keyByStream = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource();
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource();
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource();
        String head2 = curGraphHead.getId();
        /*
         * Define Source 3
         */
        SingleOutputStreamOperator<DataTuple> stream3 = createSyntheticSource();
        String head3 = curGraphHead.getId();

        /*
         * Define Source 4
         */
        SingleOutputStreamOperator<DataTuple> stream4 = createSyntheticSource();
        String head4 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                fiveWayJoin(
                        stream0,
                        stream1,
                        stream2,
                        stream3,
                        stream4,
                        head0,
                        head1,
                        head2,
                        head3,
                        head4,
                        windowOperator);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateE6WayJoin(String queryName) {
        /*
         * Drop the dice
         */
        boolean keyByStream = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            keyByStream = true;
        }

        /*
         * Define Source 0
         */
        SingleOutputStreamOperator<DataTuple> stream0 = createSyntheticSource();
        String head0 = curGraphHead.getId();

        /*
         * Define Source 1
         */
        SingleOutputStreamOperator<DataTuple> stream1 = createSyntheticSource();
        String head1 = curGraphHead.getId();

        /*
         * Define Source 2
         */
        SingleOutputStreamOperator<DataTuple> stream2 = createSyntheticSource();
        String head2 = curGraphHead.getId();
        /*
         * Define Source 3
         */
        SingleOutputStreamOperator<DataTuple> stream3 = createSyntheticSource();
        String head3 = curGraphHead.getId();

        /*
         * Define Source 4
         */
        SingleOutputStreamOperator<DataTuple> stream4 = createSyntheticSource();
        String head4 = curGraphHead.getId();

        /*
         * Define Source 5
         */
        SingleOutputStreamOperator<DataTuple> stream5 = createSyntheticSource();
        String head5 = curGraphHead.getId();

        /*
         * Join Stream0 & Stream1
         */
        WindowOperator<WindowAssigner<Object, ? extends Window>> windowOperator =
                (WindowOperator<WindowAssigner<Object, ? extends Window>>)
                        operatorProvider.provideOperator(
                                Constants.Operators.WINDOW, getOperatorIndex(false));
        SingleOutputStreamOperator<DataTuple> joinedStream =
                sixWayJoin(
                        stream0,
                        stream1,
                        stream2,
                        stream3,
                        stream4,
                        stream5,
                        head0,
                        head1,
                        head2,
                        head3,
                        head4,
                        head5,
                        windowOperator);
        /*
         * perhaps keyBy and apply windowed aggregation (same window as in join)
         */
        // ToDo: customize description to not explicitly mention window again (because it's the same
        // window as in the join)
        joinedStream = applySyntheticWindowedAggregation(joinedStream, keyByStream, windowOperator);

        joinedStream
                .addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateE2Filters(String queryName) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource();

        /*
         * Drop the dice
         */
        boolean keyBy = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            keyBy = true;
        }

        /*
         * Generate Query
         */
        stream = applySyntheticWindowedAggregation(stream, keyBy);
        // ToDo: need filter based on keyBy class?
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateE3Filters(String queryName) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource();

        /*
         * Drop the dice
         */
        boolean keyBy = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            keyBy = true;
        }

        /*
         * Generate Query
         */
        stream = applySyntheticWindowedAggregation(stream, keyBy);
        // ToDo: need filter based on keyBy class?
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }

    private void buildTemplateE4Filters(String queryName) {
        /*
         * Define Source
         */
        SingleOutputStreamOperator<DataTuple> stream = createSyntheticSource();

        /*
         * Drop the dice
         */
        boolean keyBy = randomBoolean();

        if (System.getProperty("debugMode").equals("true") || queryName.matches(".*-0")) {
            keyBy = true;
        }

        /*
         * Generate Query
         */
        stream = applySyntheticWindowedAggregation(stream, keyBy);
        // ToDo: need filter based on keyBy class?
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream = applyRandomOperator(Constants.Operators.FILTER, stream);
        stream.addSink(new SinkOperator(queryName, config).getFunction())
                .name(getOperatorIndex(false));
    }
}
