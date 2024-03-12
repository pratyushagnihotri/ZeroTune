package plangeneratorflink.operators.aggregate.functions;

import org.apache.flink.api.java.tuple.Tuple3;

import plangeneratorflink.operators.aggregate.AbstractAggregateFunction;
import plangeneratorflink.utils.DataTuple;

import java.util.ArrayList;

public class AggregateSpikeDetectionMeanFunction<S extends Number>
        extends AbstractAggregateFunction {
    /**
     * Gives the mean value(s) that is applied on a given window.
     *
     * @param klass Class type of which values to look for mean
     */
    public AggregateSpikeDetectionMeanFunction(Class<S> klass) {
        super(klass);
    }

    // avg temp value + current temp value
    @Override
    public DataTuple getResult(ArrayList<DataTuple> acc) {
        Tuple3<Integer, Integer, Integer> numTupleDatatypes =
                new Tuple3<>(1, 1, 1); // int, double, string
        tupleContent = getEmptyTupleContent(numTupleDatatypes);
        // get sums first
        Double sum = 0.0;
        Integer count = 0;
        for (DataTuple tuple : acc) {
            Double currentTemperature = tuple.getValue(Double.class, 0); // get temperature
            sum = sum + currentTemperature;
            count++;
        }
        ArrayList<Object> doubles = new ArrayList<>();
        doubles.add((sum / count)); // add average
        ArrayList<Object> integers = new ArrayList<>();
        integers.add(acc.get(0).getValue(Integer.class, 1)); // add moteid
        ArrayList<Object> strings = new ArrayList<>();
        strings.add(acc.get(0).getTimestamp()); // add timestamp
        doubles.add((sum / count));
        tupleContent.put(Double.class, doubles);
        tupleContent.put(Integer.class, integers);
        tupleContent.put(String.class, strings);
        return new DataTuple(tupleContent, findOldestTimestamp(acc));
    }
}
