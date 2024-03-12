package plangeneratorflink.utils;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static plangeneratorflink.utils.RanGen.randDoubleRange;
import static plangeneratorflink.utils.RanGen.randIntRange;
import static plangeneratorflink.utils.RanGen.randString;

public class DataTuple_Tuple extends Tuple {
    private Tuple fields; // each field needs its own description as a string
    private Tuple values;
    private Tuple3<Integer, Integer, Integer> numTupleDataTypes;

    public DataTuple_Tuple() {}

    /**
     * Constructor for new initialized and randomized DataTuples
     *
     * @param numTupleDataTypes: width that applies for each datatype (order: integer, double,
     *     string)
     */
    public DataTuple_Tuple(Tuple3<Integer, Integer, Integer> numTupleDataTypes) {
        this.numTupleDataTypes = numTupleDataTypes;
        this.fields =
                Tuple.newInstance(
                        numTupleDataTypes.f0 + numTupleDataTypes.f1 + numTupleDataTypes.f2 + 1);
        this.values =
                Tuple.newInstance(
                        numTupleDataTypes.f0 + numTupleDataTypes.f1 + numTupleDataTypes.f2 + 1);
        setTimestamp(String.valueOf(System.currentTimeMillis()));
        createRandomValues();
    }

    /**
     * Constructor for existing tuple contents. An additional timestamp value is added at first.
     *
     * @param tupleContents List of objects/values that are written into this DataTuple
     */
    public DataTuple_Tuple(
            LinkedHashMap<Class<?>, ArrayList<Object>> tupleContents, String timeStamp) {
        this.numTupleDataTypes =
                new Tuple3<>(
                        tupleContents.getOrDefault(Integer.class, new ArrayList<>()).size(),
                        tupleContents.getOrDefault(Double.class, new ArrayList<>()).size(),
                        tupleContents.getOrDefault(String.class, new ArrayList<>()).size());
        this.fields =
                Tuple.newInstance(
                        numTupleDataTypes.f0 + numTupleDataTypes.f1 + numTupleDataTypes.f2 + 1);
        this.values =
                Tuple.newInstance(
                        numTupleDataTypes.f0 + numTupleDataTypes.f1 + numTupleDataTypes.f2 + 1);
        setTimestamp(timeStamp);
        setTupleContent(tupleContents);
    }

    @Override
    public <T> T getField(int pos) {
        return values.getField(pos);
    }

    @Override
    public <T> void setField(T value, int pos) {
        values.setField(value, pos);
    }

    @Override
    public int getArity() {
        return numTupleDataTypes.f0
                + numTupleDataTypes.f1
                + numTupleDataTypes.f2
                + 1; // +1 to include timestamp
    }

    @Override
    public <T extends Tuple> T copy() {
        return (T) new DataTuple_Tuple(getTupleContent(), getTimestamp());
    }

    public void replaceTimestamp(String timestamp) {
        this.values.setField(timestamp, 0);
    }

    public String getTimestamp() {
        return this.values.getField(0);
    }

    private void setTimestamp(String timestamp) {
        this.values.setField(timestamp, 0);
        this.fields.setField("timestamp", 0);
    }

    /** Creates randomized tuple values with a fixed width per data type */
    private void createRandomValues() {
        int numInteger = this.numTupleDataTypes.f0;
        int numDouble = this.numTupleDataTypes.f1;
        int numString = this.numTupleDataTypes.f2;

        // starting at 1 to not override timestamp field (which is at index=0)
        for (int i = 1; i < numInteger; i++) {
            this.values.setField(randIntRange(Constants.Synthetic.Train.INTEGER_VALUE_RANGE), i);
            this.fields.setField(Integer.class.getSimpleName() + "-" + i, i);
        }

        for (int i = numInteger; i < numInteger + numDouble; i++) {
            this.values.setField(randDoubleRange(Constants.Synthetic.Train.DOUBLE_VALUE_RANGE), i);
            this.fields.setField(Double.class.getSimpleName() + "-" + i, i);
        }

        for (int i = numInteger + numDouble; i < numInteger + numDouble + numString; i++) {
            this.values.setField(randString(Constants.Synthetic.Train.STRING_LENGTH), i);
            this.fields.setField(String.class.getSimpleName() + "-" + i, i);
        }
    }

    public <T> T getValue(Class<T> klass, int index) {
        if (index < 0 || index >= this.values.getArity()) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Invalid Index %s for Class %s in DataTuple",
                            index, klass.getSimpleName()));
        }
        switch (klass.getSimpleName()) {
            case "Integer":
                if (index >= numTupleDataTypes.f0) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    "Invalid Index %s for Class %s in DataTuple",
                                    index, klass.getSimpleName()));
                }
                return this.values.getField(index + 1); // skip timestamp
            case "Double":
                if (index >= numTupleDataTypes.f1) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    "Invalid Index %s for Class %s in DataTuple",
                                    index, klass.getSimpleName()));
                }
                return this.values.getField(
                        index + 1 + numTupleDataTypes.f0); // skip timestamp and integers
            case "String":
                if (index >= numTupleDataTypes.f2) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    "Invalid Index %s for Class %s in DataTuple",
                                    index, klass.getSimpleName()));
                }
                return this.values.getField(
                        index
                                + 1
                                + numTupleDataTypes.f0
                                + numTupleDataTypes.f1); // skip timestamp, integers and doubles
            default:
                throw new IllegalArgumentException(
                        "Class " + klass.getSimpleName() + " is not supported");
        }
    }

    public <T, V> void setValue(Class<T> klass, int index, V value) {
        if (index < 0 || index >= this.values.getArity()) {
            throw new IndexOutOfBoundsException(
                    String.format(
                            "Invalid Index %s for Class %s in DataTuple",
                            index, klass.getSimpleName()));
        }
        switch (klass.getSimpleName()) {
            case "Integer":
                if (index >= numTupleDataTypes.f0) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    "Invalid Index %s for Class %s in DataTuple",
                                    index, klass.getSimpleName()));
                }
                this.values.setField(value, index + 1); // skip timestamp
                break;
            case "Double":
                if (index >= numTupleDataTypes.f1) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    "Invalid Index %s for Class %s in DataTuple",
                                    index, klass.getSimpleName()));
                }
                this.values.setField(
                        value, index + 1 + numTupleDataTypes.f0); // skip timestamp and integers
                break;
            case "String":
                if (index >= numTupleDataTypes.f2) {
                    throw new IndexOutOfBoundsException(
                            String.format(
                                    "Invalid Index %s for Class %s in DataTuple",
                                    index, klass.getSimpleName()));
                }
                this.values.setField(
                        value,
                        index
                                + 1
                                + numTupleDataTypes.f0
                                + numTupleDataTypes.f1); // skip timestamp, integers and doubles
                break;
            default:
                throw new IllegalArgumentException(
                        "Class " + klass.getSimpleName() + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "numTupleDataTypes: "
                + this.numTupleDataTypes
                + " (int, double, String) | fields: "
                + this.fields
                + " | values: "
                + this.values;
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
        for (int i = 1; i < getArity(); i++) { // exclude timestamp at pos 0
            Object entry = this.values.getField(i);
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
                setValue(klass, i, klass.cast(o));
                i++;
            }
        }
    }

    public Tuple getFields() {
        return fields;
    }

    public void setFields(Tuple fields) {
        this.fields = fields;
    }

    public Tuple getValues() {
        return values;
    }

    public void setValues(Tuple values) {
        this.values = values;
    }

    public Tuple3<Integer, Integer, Integer> getNumTupleDataTypes() {
        return this.numTupleDataTypes;
    }

    public void setNumTupleDataTypes(Tuple3<Integer, Integer, Integer> numTupleDataTypes) {
        this.numTupleDataTypes = numTupleDataTypes;
    }

    //    public static class DataTupleInfoFactory extends TypeInfoFactory<DataTuple> {
    //        @Override
    //        public TypeInformation<DataTuple> createTypeInfo(
    //                java.lang.reflect.Type arg0, Map<String, TypeInformation<?>> arg1) {
    //            Map<String, TypeInformation<?>> fields =
    //                    new HashMap<String, TypeInformation<?>>() {
    //                        {
    //                            put("fields", Types.LIST(Types.STRING));
    //                            put("values", Types.LIST(Types.GENERIC(Object.class)));
    //                            put(
    //                                    "numTupleDataTypes",
    //                                    TypeInformation.of(
    //                                            new TypeHint<Tuple3<Integer, Integer, Integer>>()
    // {}));
    //                        }
    //                    };
    //            return Types.POJO(DataTuple.class, fields);
    //        }
    //    }
}
