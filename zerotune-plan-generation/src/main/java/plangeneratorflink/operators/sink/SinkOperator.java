package plangeneratorflink.operators.sink;

import org.apache.flink.configuration.Configuration;

import plangeneratorflink.operators.AbstractOperator;

import java.util.HashMap;

public class SinkOperator extends AbstractOperator<SinkOperatorFunction> {

    private final String queryName;
    private Configuration config;


    public SinkOperator(String queryName, Configuration config) {
        this.function = new SinkOperatorFunction(queryName, config);
        this.queryName = queryName;
        this.config = config;
    }

    @Override
    public HashMap<String, Object> getDescription() {
        return super.getDescription();
    }


}
