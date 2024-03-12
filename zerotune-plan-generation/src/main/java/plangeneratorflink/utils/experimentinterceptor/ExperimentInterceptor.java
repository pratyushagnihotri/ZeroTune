package plangeneratorflink.utils.experimentinterceptor;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

/**
 * With ExperimentInterceptor instances you can adapt several parameters inside a query, that are
 * normally not adjustable. This is done by changing the config value between every query run. The
 * queryBuilder checks if there is a custom value set in the config and then takes this one. Be
 * aware, that this is not implemented everywhere and depends on the query implementation. It is
 * mostly designed for the synthetic test queries.
 */
public abstract class ExperimentInterceptor {

    protected int start;
    protected int step;
    protected Configuration config;

    public ExperimentInterceptor(Configuration config, int start, int step) {
        this.config = config;
        this.start = start;
        this.step = step;
    }

    public static ExperimentInterceptor getInterceptor(Configuration config) {
        String interceptorType =
                config.getString(ConfigOptions.key("interceptor").stringType().noDefaultValue());
        int start =
                config.getInteger(ConfigOptions.key("interceptorStart").intType().noDefaultValue());
        int step =
                config.getInteger(ConfigOptions.key("interceptorStep").intType().noDefaultValue());
        ExperimentInterceptor ei = null;
        switch (interceptorType) {
            case "eventrate":
                return new EventRateInterceptor(config, start, step);
        }
        return null;
    }

    public abstract void intercept(int runIndex);

    public abstract void reset();
}
