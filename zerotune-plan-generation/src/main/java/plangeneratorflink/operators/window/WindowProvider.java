package plangeneratorflink.operators.window;

import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;

import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.RanGen;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

/**
 * Generates random window operator to be used in the {@link
 * plangeneratorflink.querybuilder.AbstractQueryBuilder}.executeWindowAggregate() or executeJoin().
 */
public class WindowProvider {
    private ArrayList<WindowOperator<WindowAssigner<Object, ? extends Window>>> operators;

    public WindowProvider() {
        operators = new ArrayList<>();
        /*
         * Tumbling Time Window
         */
        operators.add(getTumblingTimeWindowOperator(getRandomDurationWindow()));

        /*
         * Sliding Time Window
         */
        Time slidingDurationWindow = getRandomDurationWindow();
        operators.add(
                getSlidingTimeWindowOperator(
                        slidingDurationWindow,
                        getRandomDurationSlidingSize(slidingDurationWindow)));

        /*
         * Tumbling Count Window
         */
        operators.add(getTumblingCountWindowOperator(getRandomCountWindowSize()));

        /*
         * Sliding Count Window
         */
        Integer slidingCountWindowSize = getRandomCountWindowSize();
        operators.add(
                getSlidingCountWindowOperator(
                        slidingCountWindowSize, getRandomCountSlidingSize(slidingCountWindowSize)));
    }

    public WindowOperator<WindowAssigner<Object, ? extends Window>> getTumblingTimeWindowOperator(
            Time windowDuration) {
        return new WindowOperator<>(
                "tumblingWindow",
                "duration",
                TumblingProcessingTimeWindows.of(windowDuration),
                (int) windowDuration.toMilliseconds(),
                null);
    }

    public WindowOperator<WindowAssigner<Object, ? extends Window>> getSlidingTimeWindowOperator(
            Time windowDuration, Time slidingDuration) {
        return new WindowOperator<>(
                "slidingWindow",
                "duration",
                SlidingProcessingTimeWindows.of(windowDuration, slidingDuration),
                (int) windowDuration.toMilliseconds(),
                (int) slidingDuration.toMilliseconds());
    }

    public WindowOperator<WindowAssigner<Object, ? extends Window>> getTumblingCountWindowOperator(
            Integer windowSize) {
        return new WindowOperator<>(
                "tumblingWindow", "count", TumblingCountWindows.of(windowSize), windowSize, null);
    }

    public WindowOperator<WindowAssigner<Object, ? extends Window>> getSlidingCountWindowOperator(
            Integer windowSize, Integer slideSize) {
        // Attention: To use this window an evictor is needed
        // (.evictor(windowOperator.getEvictor()))
        WindowOperator<WindowAssigner<Object, ? extends Window>> slidingCountWindowOperator =
                new WindowOperator<>(
                        "slidingWindow",
                        "count",
                        SlidingCountWindows.of(windowSize, slideSize),
                        windowSize,
                        slideSize);
        slidingCountWindowOperator.setEvictor(CountEvictor.of(windowSize));
        return slidingCountWindowOperator;
    }

    private Time getRandomDurationSlidingSize(Time durationWindow) {
        return Time.of(
                (int)
                        (durationWindow.toMilliseconds()
                                * RanGen.randDoubleFromList(
                                        Constants.Synthetic.Train.WINDOW_SLIDING_RATIO)),
                TimeUnit.MILLISECONDS);
    }

    private Time getRandomDurationWindow() {
        return Time.milliseconds(RanGen.randIntFromList(Constants.Synthetic.Train.WINDOW_DURATION));
    }

    private Integer getRandomCountWindowSize() {
        return RanGen.randIntFromList(Constants.Synthetic.Train.WINDOW_LENGTH);
    }

    private Integer getRandomCountSlidingSize(int windowSize) {
        return (int)
                (windowSize
                        * RanGen.randDoubleFromList(
                                Constants.Synthetic.Train.WINDOW_SLIDING_RATIO));
    }

    public WindowOperator<WindowAssigner<Object, ? extends Window>> getRandomWindowOperator(
            boolean deterministic) {
        if (deterministic) {
            int windowSize = Constants.Synthetic.Train.DeterministicParameter.WINDOW_SIZE;
            int slidingSize =
                    (int)
                            (windowSize
                                    * Constants.Synthetic.Train.DeterministicParameter
                                            .WINDOW_SLIDING_RATIO);
            switch (Constants.Synthetic.Train.DeterministicParameter.WINDOW_TYPE_INDEX) {
                case 0:
                    return getTumblingTimeWindowOperator(Time.milliseconds(windowSize));
                case 1:
                    return getSlidingTimeWindowOperator(
                            Time.milliseconds(windowSize), Time.milliseconds(slidingSize));
                case 2:
                    return getTumblingCountWindowOperator(windowSize);
                default:
                    return getSlidingCountWindowOperator(windowSize, slidingSize);
            }
        } else {
            return operators.get(RanGen.randInt(0, operators.size() - 1));
        }
    }

    public WindowOperator<WindowAssigner<Object, ? extends Window>> getRandomTimeBasedWindowOp(
            int windowLength) {
        if (RanGen.randomBoolean()) {
            return getTumblingTimeWindowOperator(Time.milliseconds(windowLength));
        } else {
            return getSlidingTimeWindowOperator(
                    Time.milliseconds(windowLength),
                    Time.milliseconds(getRandomCountSlidingSize(windowLength)));
        }
    }

    public WindowOperator<WindowAssigner<Object, ? extends Window>> getRandomCountBasedWindowOp(
            int windowLength) {
        if (RanGen.randomBoolean()) {
            return getTumblingCountWindowOperator(windowLength);
        } else {
            return getSlidingCountWindowOperator(
                    windowLength, getRandomCountSlidingSize(windowLength));
        }
    }
}
