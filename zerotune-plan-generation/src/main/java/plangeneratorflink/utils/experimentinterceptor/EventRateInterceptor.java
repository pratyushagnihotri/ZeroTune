package plangeneratorflink.utils.experimentinterceptor;

import org.apache.flink.configuration.Configuration;

/** Intercepts the event rate between runs and adapts it increasingly. */
public class EventRateInterceptor extends ExperimentInterceptor {

    private int eventRate = start;

    public EventRateInterceptor(Configuration config, int start, int step) {
        super(config, start, step);
    }

    @Override
    public void intercept(int runIndex) {
        config.setInteger("eventRate", eventRate);
        eventRate += step;
    }

    @Override
    public void reset() {
        eventRate = start;
    }
}
