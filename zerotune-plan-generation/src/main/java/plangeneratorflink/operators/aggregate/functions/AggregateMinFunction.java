package plangeneratorflink.operators.aggregate.functions;

import java.util.ArrayList;

import org.apache.flink.api.java.tuple.Tuple3;

import plangeneratorflink.operators.aggregate.AbstractAggregateFunction;
import plangeneratorflink.utils.DataTuple;

public class AggregateMinFunction<S extends Number> extends AbstractAggregateFunction {

    public AggregateMinFunction(Class<S> klass) {
        super(klass);
    }

    @Override
    public DataTuple getResult(ArrayList<DataTuple> acc) {
        DataTuple firstTuple = acc.get(0);
        Tuple3<Integer, Integer, Integer> numTupleDatatypes = firstTuple.getNumTupleDataTypes();
        tupleContent = getEmptyTupleContent(numTupleDatatypes);
        int numOfValuesInClass = firstTuple.maxAmountOfDataType(klass);
        ArrayList<Object> lowestValues = this.getInitList(numOfValuesInClass, 100000);

        for (DataTuple tuple : acc) {
            ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
            for (int i = 0; i < classContent.size(); i++) {
                Object result = lowestValues.get(i);
                if (klass == Integer.class) {
                    if ((int) classContent.get(i) < (int) lowestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else if (klass == Double.class) {
                    if ((double) classContent.get(i) < (double) lowestValues.get(i)) {
                        result = classContent.get(i);
                    }
                } else {
                    throw new RuntimeException("Class is not defined");
                }
                lowestValues.set(i, result);
            }
            tupleContent.put(klass, lowestValues);
        }
        // use just oldest timestamp
        return new DataTuple(tupleContent, findOldestTimestamp(acc));
    }

}
