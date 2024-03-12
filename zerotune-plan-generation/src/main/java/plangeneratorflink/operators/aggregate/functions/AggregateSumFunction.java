package plangeneratorflink.operators.aggregate.functions;

import java.util.ArrayList;

import org.apache.flink.api.java.tuple.Tuple3;

import plangeneratorflink.operators.aggregate.AbstractAggregateFunction;
import plangeneratorflink.utils.DataTuple;

public class AggregateSumFunction<T> extends AbstractAggregateFunction {
  /**
   * Sum for each tuple field of integers or doubles along a window
   *
   * @param klass Class of values to to sum up
   */
  public AggregateSumFunction(Class<T> klass) {
    super(klass);
  }

  @Override
  public DataTuple getResult(ArrayList<DataTuple> acc) {
    DataTuple firstTuple = acc.get(0);
    Tuple3<Integer, Integer, Integer> numTupleDatatypes = firstTuple.getNumTupleDataTypes();
    tupleContent = getEmptyTupleContent(numTupleDatatypes);
    int numOfValuesInClass = firstTuple.maxAmountOfDataType(klass);
    ArrayList<Object> sums = this.getInitList(numOfValuesInClass, 0);

    for (DataTuple tuple : acc) {
      ArrayList<Object> classContent = tuple.getTupleContent().get(klass);
      for (int i = 0; i < classContent.size(); i++) {
        Object result = null;
        if (klass == Integer.class) {
          result = (int) sums.get(i) + (int) tuple.getValue(klass, i);
        }
        if (klass == Double.class) {
          result = (double) sums.get(i) + (double) tuple.getValue(klass, i);
        }
        sums.set(i, result);
      }
    }
    tupleContent.put(klass, sums);
    return new DataTuple(tupleContent, findOldestTimestamp(acc));
  }

}
