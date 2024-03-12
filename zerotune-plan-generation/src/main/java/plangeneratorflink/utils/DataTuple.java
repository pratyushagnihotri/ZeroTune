package plangeneratorflink.utils;

import org.apache.flink.api.common.typeinfo.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static plangeneratorflink.utils.RanGen.*;

@TypeInfo(DataTuple.DataTupleInfoFactory.class)
public class DataTuple extends Tuple {
    private final ArrayList<String> fields =
            new ArrayList<>(); // each field needs its own description as a string
    private final ArrayList<Object> values = new ArrayList<>();
    private final Tuple3<Integer, Integer, Integer> numTupleDataTypes;


    public DataTuple() {
        this.numTupleDataTypes = new Tuple3<>(0, 0, 0);
    }

    /**
     * Constructor for new initialized and randomized DataTuples
     *
     * @param numTupleDatatypes: width that applies for each datatype
     */
    public DataTuple(Tuple3<Integer, Integer, Integer> numTupleDatatypes) {
        this.numTupleDataTypes = numTupleDatatypes;
        setTimestamp(String.valueOf(System.currentTimeMillis()));
        createRandomValues();
    }

    /**
     * Constructor for existing tuple contents. An additional timestamp value is added at first.
     *
     * @param tupleContents List of objects/values that are written into this DataTuple
     */
    public DataTuple(LinkedHashMap<Class<?>, ArrayList<Object>> tupleContents, String timeStamp) {
        this.numTupleDataTypes = new Tuple3<>(
                tupleContents.getOrDefault(Integer.class, new ArrayList<>()).size(),
                tupleContents.getOrDefault(Double.class, new ArrayList<>()).size(),
                tupleContents.getOrDefault(String.class, new ArrayList<>()).size());
        setTimestamp(timeStamp);
        setTupleContent(tupleContents);
    }

    @Override
    public <T> T getField(int pos) {
        return (T) values.get(pos);
    }

    @Override
    public <T> void setField(T value, int pos) {
        values.set(pos, value);
    }

    @Override
    public int getArity() {
        return numTupleDataTypes.f0 + numTupleDataTypes.f1 + numTupleDataTypes.f2 + 1;
    } // +1 to include timestamp

    @Override
    public <T extends Tuple> T copy() {
        return (T) new DataTuple(getTupleContent(), getTimestamp());
    }


    public Tuple3<Integer, Integer, Integer> getNumTupleDataTypes() {
        return this.numTupleDataTypes;
    }

    private void setTimestamp(String timestamp) {
        this.values.add(timestamp);
        this.fields.add("timestamp");
    }

    public void replaceTimestamp(String timestamp) {
        this.values.remove(0);
        this.values.add(0, timestamp);
    }

    public String getTimestamp() {
        return (String) this.values.get(0);
    }

    /**
     * Creates randomized tuple values with a fixed width per data type
     */
    private void createRandomValues() {
        int numInteger = this.numTupleDataTypes.f0;
        int numDouble = this.numTupleDataTypes.f1;
        int numString = this.numTupleDataTypes.f2;

        for (int i = 0; i < numInteger; i++) {
            this.values.add(randIntRange(Constants.Synthetic.Train.INTEGER_VALUE_RANGE));
            this.fields.add(Integer.class.getSimpleName() + "-" + i);
        }

        for (int i = 0; i < numDouble; i++) {
            this.values.add(randDoubleRange(Constants.Synthetic.Train.DOUBLE_VALUE_RANGE));
            this.fields.add(Double.class.getSimpleName() + "-" + i);
        }

        for (int i = 0; i < numString; i++) {
            this.values.add(randString(Constants.Synthetic.Train.STRING_LENGTH));
            this.fields.add(String.class.getSimpleName() + "-" + i);
        }
    }

    public ArrayList<String> getFields() {
        return fields;
    }

    public void setFields(ArrayList<String> fields) {
        this.fields.clear();
        this.fields.addAll(fields);
    }

    public ArrayList<Object> getValues() {
        return this.values;
    }

    public void setValues(ArrayList<Object> values) {
        this.values.clear();
        this.values.addAll(values);
    }

    public <T> T getValue(Class<T> klass, int index) {
        if (index < 0 || index >= this.values.size()) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Invalid Index %s for Class %s in DataTuple", index, klass.getSimpleName()));
        }
        int fieldIndex = this.fields.indexOf(klass.getSimpleName() + "-" + index);
        if (fieldIndex == -1) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Invalid Index %s for Class %s in DataTuple", index, klass.getSimpleName()));
        }
        return klass.cast(this.values.get(fieldIndex));
    }

    public void setNumTupleDatatypes(Tuple3<Integer, Integer, Integer> numTupleDataTypes) {
        this.numTupleDataTypes.f0 = numTupleDataTypes.f0;
        this.numTupleDataTypes.f1 = numTupleDataTypes.f1;
        this.numTupleDataTypes.f2 = numTupleDataTypes.f2;
    }

    public static class DataTupleInfoFactory extends TypeInfoFactory<DataTuple> {
        @Override
        public TypeInformation<DataTuple> createTypeInfo(java.lang.reflect.Type arg0,
                                                         Map<String, TypeInformation<?>> arg1) {
            Map<String, TypeInformation<?>> fields =
                    new HashMap<String, TypeInformation<?>>() {
                        {
                            put("fields", Types.LIST(Types.STRING));
                            put("values", Types.LIST(Types.GENERIC(Object.class)));
                            put("numTupleDataTypes", TypeInformation.of(new TypeHint<Tuple3<Integer, Integer, Integer>>() {
                            }));
                        }
                    };
            return Types.POJO(DataTuple.class, fields);
        }
    }

    @Override
    public String toString() {
        return "numTupleDataTypes: " + this.numTupleDataTypes + " (int, double, String) | fields: " + this.fields + " | values: " + this.values;
    }

    public int maxAmountOfDataType(Class<?> klass) {
        if (klass == Integer.class) {
            return this.numTupleDataTypes.f0;
        } else if (klass == Double.class) {
            return this.numTupleDataTypes.f1;
        } else if (klass == String.class) {
            return this.numTupleDataTypes.f2;
        }
        return -1;
    }

    public LinkedHashMap<Class<?>, ArrayList<Object>> getTupleContent() {
        LinkedHashMap<Class<?>, ArrayList<Object>> tupleContent = new LinkedHashMap<>();
        for (Object entry : this.values.subList(1, this.values.size())) { //exclude timestamp
            if (entry != null) {
                Class<?> klass = entry.getClass();
                ArrayList<Object> classList = tupleContent.getOrDefault(klass, new ArrayList<>());
                classList.add(entry);
                tupleContent.put(klass, classList);
            }
        }
        return tupleContent;
    }

    public void setTupleContent(HashMap<Class<?>, ArrayList<Object>> tupleContents) {
        for (Map.Entry<Class<?>, ArrayList<Object>> entry : tupleContents.entrySet()) {
            int i = 0;
            Class<?> klass = entry.getKey();
            ArrayList<Object> classContents = tupleContents.get(klass);
            for (Object o : classContents) {
                this.values.add(klass.cast(o));
                this.fields.add((klass.getSimpleName() + "-" + i));
                i++;
            }
        }
    }

}
