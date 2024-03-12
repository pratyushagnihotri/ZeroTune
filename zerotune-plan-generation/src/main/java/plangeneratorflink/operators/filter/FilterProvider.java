package plangeneratorflink.operators.filter;

import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;
import plangeneratorflink.utils.RanGen;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/** Provides different filters. * */
public class FilterProvider {
    private ArrayList<Class<?>> supportedClasses;
    private ArrayList<FilterOperator<?>> operators;

    public FilterProvider() {
        supportedClasses = new ArrayList<>();
        operators = new ArrayList<>();
        this.supportedClasses.addAll(Arrays.asList(Integer.class, Double.class, String.class));

        operators.add(
                new FilterOperator<String>(
                        (BiFunction<String, String, Boolean> & Serializable) String::startsWith,
                        "startsWith",
                        String.class));

        operators.add(
                new FilterOperator<String>(
                        (BiFunction<String, String, Boolean> & Serializable) String::endsWith,
                        "endsWith",
                        String.class));

        operators.add(
                new FilterOperator<String>(
                        (BiFunction<String, String, Boolean> & Serializable)
                                (x, y) -> !x.startsWith(y),
                        "endsNotWith",
                        String.class));

        operators.add(
                new FilterOperator<String>(
                        (BiFunction<String, String, Boolean> & Serializable)
                                (x, y) -> !x.startsWith(y),
                        "startsNotWith",
                        String.class));
        /*
         * operators.add(
         * new FilterOperator(
         * (BiFunction<String, String, Boolean> & Serializable) String::equals,
         * "equals",
         * String.class));
         */
        operators.add(
                new FilterOperator<String>(
                        (BiFunction<String, String, Boolean> & Serializable) String::contains,
                        "contains",
                        String.class));

        /*
         * operators.add(
         * new FilterOperator(
         * (BiFunction<Integer, Integer, Boolean> & Serializable) Integer::equals,
         * "equals",
         * Integer.class));
         */

        operators.add(
                new FilterOperator<Integer>(
                        (BiFunction<Integer, Integer, Boolean> & Serializable)
                                (x, y) -> !x.equals(y),
                        "notEquals",
                        Integer.class));

        operators.add(
                new FilterOperator<Integer>(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x > y,
                        "greaterThan",
                        Integer.class));

        operators.add(
                new FilterOperator<Integer>(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x < y,
                        "lessThan",
                        Integer.class));

        operators.add(
                new FilterOperator<Integer>(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x >= y,
                        "greaterEquals",
                        Integer.class));

        operators.add(
                new FilterOperator<Integer>(
                        (BiFunction<Integer, Integer, Boolean> & Serializable) (x, y) -> x <= y,
                        "lessEquals",
                        Integer.class));

        /*
         * operators.add(
         * new FilterOperator(
         * (BiFunction<Double, Double, Boolean> & Serializable) Double::equals,
         * "equals",
         * Double.class));
         */

        operators.add(
                new FilterOperator<Double>(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> !x.equals(y),
                        "notEquals",
                        Double.class));

        operators.add(
                new FilterOperator<Double>(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x > y,
                        "greaterThan",
                        Double.class));

        operators.add(
                new FilterOperator<Double>(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x < y,
                        "lessThan",
                        Double.class));

        operators.add(
                new FilterOperator<Double>(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x >= y,
                        "greaterEquals",
                        Double.class));

        operators.add(
                new FilterOperator<Double>(
                        (BiFunction<Double, Double, Boolean> & Serializable) (x, y) -> x <= y,
                        "lessEquals",
                        Double.class));
    }

    public FilterOperator<DataTuple> getRandomFilterFunction(
            Class<?> klass, boolean deterministic) {
        List<FilterOperator<?>> operatorsOfKlass;
        if (klass != null) {
            operatorsOfKlass =
                    operators.stream()
                            .filter(operator -> operator.getFilterKlass().equals(klass))
                            .collect(Collectors.toList());
        } else {
            operatorsOfKlass = operators;
        }
        if (deterministic) {
            String filterName =
                    Constants.Synthetic.Train.DeterministicParameter.FILTER_FUNCTION_NAME;
            Class<?> filterClass = Constants.Synthetic.Train.DeterministicParameter.FILTER_CLASS;
            FilterOperator<DataTuple> filter = null;
            if (klass == null) {
                filter =
                        (FilterOperator<DataTuple>)
                                operators.stream()
                                        .filter(
                                                operator ->
                                                        operator.getFilterKlass()
                                                                .equals(filterClass))
                                        .filter(
                                                operator ->
                                                        operator.getFilterType().equals(filterName))
                                        .collect(Collectors.toList())
                                        .get(0);
            } else {

                List<FilterOperator<?>> filteredFilters =
                        operatorsOfKlass.stream()
                                .filter(operator -> operator.getFilterType().equals(filterName))
                                .collect(Collectors.toList());
                if (filteredFilters.size() > 0) {
                    filter = (FilterOperator<DataTuple>) filteredFilters.get(0);
                } else {
                    filter = (FilterOperator<DataTuple>) operatorsOfKlass.get(0);
                }
            }
            if (filter.getFilterKlass() == String.class) {
                filter.setLiteral(
                        Constants.Synthetic.Train.DeterministicParameter.FILTER_LITERAL_STRING);
            }
            if (filter.getFilterKlass() == Integer.class) {
                filter.setLiteral(
                        Constants.Synthetic.Train.DeterministicParameter.FILTER_LITERAL_INTEGER);
            }
            if (filter.getFilterKlass() == Double.class) {
                filter.setLiteral(
                        Constants.Synthetic.Train.DeterministicParameter.FILTER_LITERAL_DOUBLE);
            }
            filter.setFieldNumber(
                    Constants.Synthetic.Train.DeterministicParameter.FILTER_FIELD_NUMBER);
            return filter;
        } else {
            return (FilterOperator<DataTuple>)
                    operatorsOfKlass.get(RanGen.randInt(0, operatorsOfKlass.size() - 1));
        }
    }

    public ArrayList<FilterOperator<?>> getOperators() {
        return operators;
    }
}
