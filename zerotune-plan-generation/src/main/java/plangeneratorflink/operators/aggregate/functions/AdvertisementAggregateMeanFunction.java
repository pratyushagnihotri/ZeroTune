package plangeneratorflink.operators.aggregate.functions;


import org.apache.flink.api.java.tuple.Tuple3;
import plangeneratorflink.operators.aggregate.AbstractAggregateFunction;
import plangeneratorflink.utils.DataTuple;

import java.util.ArrayList;

public class AdvertisementAggregateMeanFunction<S extends Number> extends AbstractAggregateFunction {
    /**
     * Gives the mean value(s) that is applied on a given window.
     * Difference to AggregateMeanFunction is, that the first String value result also in the resulting DataTuple.
     *
     * @param klass Class type of which values to look for mean
     */
    public AdvertisementAggregateMeanFunction(Class<S> klass) {
        super(klass);
    }

    @Override
    public DataTuple getResult(ArrayList<DataTuple> acc) {
        DataTuple firstTuple = acc.get(0);
        Tuple3<Integer, Integer, Integer> numTupleDatatypes = firstTuple.getNumTupleDataTypes();
        tupleContent = getEmptyTupleContent(numTupleDatatypes);
        int numOfValuesInClass = firstTuple.maxAmountOfDataType(klass);
        ArrayList<Object> means = this.getInitList(numOfValuesInClass, 0);
        // get sums first
        for (DataTuple tuple : acc) {
            ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
            for (int i = 0; i < classContent.size(); i++) {
                Object result = null;
                if (klass == Integer.class) {
                    result = (int) means.get(i) + (int) tuple.getValue(klass, i);
                }
                if (klass == Double.class) {
                    result = (double) means.get(i) + (double) tuple.getValue(klass, i);
                }
                means.set(i, result);
            }
        }

        // get means
        for (int i = 0; i < means.size(); i++) {
            Object result = null;
            if (klass == Integer.class) {
                result = (int) means.get(i) / acc.size();
            }
            if (klass == Double.class) {
                result = (double) means.get(i) / acc.size();
            }
            means.set(i, result);
        }
        tupleContent.put(klass, means);
        //add queryID for advertisement
        tupleContent.get(String.class).set(0, acc.get(0).getValue(String.class, 0));
        return new DataTuple(tupleContent, findOldestTimestamp(acc));
    }

}
