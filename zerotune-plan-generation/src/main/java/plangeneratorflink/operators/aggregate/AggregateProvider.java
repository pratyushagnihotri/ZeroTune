package plangeneratorflink.operators.aggregate;

import plangeneratorflink.operators.aggregate.functions.AggregateMaxFunction;
import plangeneratorflink.operators.aggregate.functions.AggregateMeanFunction;
import plangeneratorflink.operators.aggregate.functions.AggregateMinFunction;
import plangeneratorflink.operators.aggregate.functions.AggregateSumFunction;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.RanGen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.stream.Collectors;

public class AggregateProvider {

    private ArrayList<Class<?>> supportedClasses;
    private ArrayList<AggregateOperator> operators;

    public AggregateProvider() {
        supportedClasses = new ArrayList<>();
        operators = new ArrayList<>();
        this.supportedClasses.addAll(Arrays.asList(Integer.class, Double.class));
        operators.add(
                new AggregateOperator(
                        new AggregateSumFunction<>(Integer.class), "sum", Integer.class));
        operators.add(
                new AggregateOperator(
                        new AggregateSumFunction<>(Double.class), "sum", Double.class));
        operators.add(
                new AggregateOperator(
                        new AggregateMeanFunction<>(Double.class), "mean", Double.class));
        operators.add(
                new AggregateOperator(
                        new AggregateMeanFunction<>(Integer.class), "mean", Integer.class));
        operators.add(
                new AggregateOperator(
                        new AggregateMaxFunction<>(Integer.class), "max", Integer.class));
        operators.add(
                new AggregateOperator(
                        new AggregateMaxFunction<>(Double.class), "max", Double.class));
        operators.add(
                new AggregateOperator(
                        new AggregateMinFunction<>(Integer.class), "min", Integer.class));
        operators.add(
                new AggregateOperator(
                        new AggregateMinFunction<>(Double.class), "min", Double.class));
    }

    public AggregateOperator getRandomAggregateFunction(boolean deterministic) {
        AggregateOperator aggregateFunction;
        if (deterministic) {
            aggregateFunction =
                    operators.get(
                            Constants.Synthetic.Train.DeterministicParameter
                                    .AGGREGATE_FUNCTION_INDEX);
        } else {
            aggregateFunction = operators.get(RanGen.randInt(0, operators.size() - 1));
        }
        return aggregateFunction;
    }

    public AggregateOperator getAggregateFunction(String aggFunctionName, String klass) {
        return operators.stream()
                .filter(
                        aggregateOperator ->
                                aggregateOperator.getAggFunction().equals(aggFunctionName)
                                        && aggregateOperator.getKlass().getSimpleName().equals(klass))
                .collect(Collectors.toList())
                .get(0);
    }
}
