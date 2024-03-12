package plangeneratorflink.operators.aggregate;

import java.util.ArrayList;
import java.util.LinkedHashMap;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;

import plangeneratorflink.utils.DataTuple;

public abstract class AbstractAggregateFunction implements AggregateFunction<DataTuple, ArrayList<DataTuple>, DataTuple> {

    public final Class<?> klass;
    public LinkedHashMap<Class<?>, ArrayList<Object>> tupleContent = new LinkedHashMap<>();

    public AbstractAggregateFunction(Class<?> klass){
        this.klass = klass;
    }

    @Override
    public ArrayList<DataTuple> add(DataTuple value, ArrayList<DataTuple> acc) {
       acc.add(value);
       return acc;
    }

    @Override
    public ArrayList<DataTuple> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public ArrayList<DataTuple> merge(ArrayList<DataTuple> acc1, ArrayList<DataTuple> acc2) {
        acc1.addAll(acc2);
        return acc1;
    }

    /**
     * Initizalizes an array list with specified values, e.g. an array of [0.0, 0.0...]
     * @param len Length of array
     * @param val Value to insert
     * @return array list
     */
    public ArrayList<Object> getInitList(int len, int val) {
        ArrayList<Object> initList = new ArrayList<>();
        if (klass == Integer.class) {
            for (int i = 0; i < len; i++) {
                initList.add(klass.cast(val));
            }
        } else if (klass == Double.class) {
            for (int i = 0; i < len; i++) {
                initList.add((double) val);
            }
        }
        return initList;
    }

    public String findOldestTimestamp(ArrayList<DataTuple> tupleList){
        long timestamp = Long.parseLong(tupleList.get(0).getTimestamp());
        for (DataTuple tuple : tupleList){
            if (Long.parseLong(tuple.getTimestamp()) < timestamp){
                timestamp = Long.parseLong(tuple.getTimestamp());
            }
        }
        return String.valueOf(timestamp);
    }

    /**
     * This creates an empty tuple content. For each tuple class, there are a specific amount of zeros added
     * @param numTupleDatatypes number of values per datatype/class.
     * @return empty tuple content
     */
    public LinkedHashMap<Class<?>, ArrayList<Object>> getEmptyTupleContent(Tuple3<Integer, Integer, Integer> numTupleDatatypes) {
        LinkedHashMap<Class<?>, ArrayList<Object>> tupleContent = new LinkedHashMap<>();
        ArrayList<Object> klassList = new ArrayList<>();

        for (int i = 0; i < numTupleDatatypes.f0; i++) {
            klassList.add(null);
        }
        tupleContent.put(Integer.class, new ArrayList<>(klassList));

        klassList.clear();
        for (int i = 0; i < numTupleDatatypes.f1; i++) {
            klassList.add(null);
        }
        tupleContent.put(Double.class, new ArrayList<>(klassList));

        klassList.clear();
        for (int i = 0; i < numTupleDatatypes.f2; i++) {
            klassList.add(null);
        }
        tupleContent.put(String.class, new ArrayList<>(klassList));

        return tupleContent;
    }

    
    
}
