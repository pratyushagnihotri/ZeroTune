package plangeneratorflink.operators;

import plangeneratorflink.operators.aggregate.AggregateProvider;
import plangeneratorflink.operators.filter.FilterProvider;
import plangeneratorflink.operators.window.WindowProvider;
import plangeneratorflink.utils.Constants;

public class OperatorProvider {

    private final FilterProvider filterProvider;
    private final WindowProvider windowProvider;
    private final AggregateProvider aggregateProvider;

    private Class<?> aggregationClass; // this is the class that remains after aggregation

    public OperatorProvider() {
        filterProvider = new FilterProvider();
        windowProvider = new WindowProvider();
        aggregateProvider = new AggregateProvider();
    }

    /**
     * Provides a random operator of given type with a given index. Note that depending on previous
     * operators, the output depends. If an aggregation is performed on a specific class, a
     * subsequent filter will also be only applied on that class.
     *
     * @param type Name of the operator
     * @param index Index (=id) of the operator
     * @return random operator of given type
     */
    public AbstractOperator<?> provideOperator(String type, String index) {
        return provideOperator(type, index, false);
    }

    public AbstractOperator<?> provideOperator(String type, String index, boolean deterministic) {
        AbstractOperator<?> operator;
        switch (type) {
            case Constants.Operators.FILTER:
                operator = filterProvider.getRandomFilterFunction(aggregationClass, deterministic);
                break;
            case Constants.Operators.AGGREGATE:
                operator = aggregateProvider.getRandomAggregateFunction(deterministic);
                aggregationClass = operator.getKlass();
                break;
            case Constants.Operators.WINDOW:
                operator = windowProvider.getRandomWindowOperator(deterministic);
                break;
            default:
                throw new IllegalStateException("Unexpected value: " + type);
        }
        operator.setId(index); // create unique identifier for operator
        return operator;
    }
}
