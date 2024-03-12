package plangeneratorflink.operators.aggregate.functions;

import java.util.ArrayList;

import org.apache.flink.api.java.tuple.Tuple3;

import plangeneratorflink.operators.aggregate.AbstractAggregateFunction;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

public class AggregateMaxFunction<S extends Number> extends AbstractAggregateFunction {
    /**
     * Gives the maximum value(s) that is applied on a given window.
     *
     * @param klass Class type of which values to look for maximum
     */
    public AggregateMaxFunction(Class<S> klass) {
        super(klass);
    }

    @Override
    public DataTuple getResult(ArrayList<DataTuple> acc) {
        DataTuple firstTuple = acc.get(0);
        Tuple3<Integer, Integer, Integer> numTupleDatatypes = firstTuple.getNumTupleDataTypes();
        tupleContent = getEmptyTupleContent(numTupleDatatypes);
        int numOfValuesInClass = firstTuple.maxAmountOfDataType(klass);
        ArrayList<Object> highestValues = this.getInitList(numOfValuesInClass, Constants.Synthetic.Train.INTEGER_VALUE_RANGE.f0);

        for (DataTuple tuple : acc) {
            ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
            for (int i = 0; i < classContent.size(); i++) {
                Object result = highestValues.get(i);
                if (klass == Integer.class) {
                    if ((int) classContent.get(i) > (int) highestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else if (klass == Double.class) {
                    if ((double) classContent.get(i) > (double) highestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else {
                    throw new RuntimeException("Class is not defined");
                }
                highestValues.set(i, result);
            }
            tupleContent.put(klass, highestValues);

        }
        // use just oldest timestamp
        return new DataTuple(tupleContent, findOldestTimestamp(acc));
    }

}