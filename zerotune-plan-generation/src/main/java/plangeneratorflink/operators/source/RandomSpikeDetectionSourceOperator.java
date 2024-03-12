package plangeneratorflink.operators.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.DataTuple;
import plangeneratorflink.utils.RanGen;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.concurrent.ThreadLocalRandom;

public class RandomSpikeDetectionSourceOperator extends AbstractOperator<DataGeneratorSource<DataTuple>> implements DataGenerator<DataTuple> {
    private Tuple3<Integer, Integer, Integer> numTupleDatatypes; // int, double, string

    private int rowsPerSecond;
    private DataGeneratorSource<DataTuple> dataGeneratorSource;
    private ArrayList<String> deviceIds;

    public RandomSpikeDetectionSourceOperator(String id, int rowsPerSecond) {
        setId(id);
        this.numTupleDatatypes = new Tuple3<>(0, 1, 2);
        this.rowsPerSecond = rowsPerSecond;
        dataGeneratorSource = new DataGeneratorSource<>(this, rowsPerSecond, null, getDescription());
        this.function = dataGeneratorSource;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public DataTuple next() {
        LinkedHashMap<Class<?>, ArrayList<Object>> tupleContent = new LinkedHashMap<>();
        ArrayList<Object> doubleArrayList = new ArrayList<>();
        ArrayList<Object> integerArrayList = new ArrayList<>();
        ArrayList<Object> stringArrayList = new ArrayList<>();

        stringArrayList.add(deviceIds.get(ThreadLocalRandom.current().nextInt(deviceIds.size())));  //deviceID
        stringArrayList.add(String.valueOf(new Date()));    //date
        doubleArrayList.add((RanGen.randDouble() * 10) + 50);  //value

        tupleContent.put(Double.class, doubleArrayList);
        tupleContent.put(Integer.class, integerArrayList);
        tupleContent.put(String.class, stringArrayList);

        return new DataTuple(tupleContent, String.valueOf(System.currentTimeMillis()));
    }

    @Override
    public void open(String name, FunctionInitializationContext context, RuntimeContext runtimeContext) throws Exception {
        this.deviceIds = new ArrayList<>();
        for (int i = 0; i <= 5; i++) {
            deviceIds.add(RanGen.randString(20));
        }
    }

    public Tuple3<Integer, Integer, Integer> getNumTupleDatatypes() {
        return this.numTupleDatatypes;
    }


    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", "SourceOperator");
        description.put("confEventRate", this.rowsPerSecond);
        description.put("numInteger", getNumTupleDatatypes().f0);
        description.put("numDouble", getNumTupleDatatypes().f1);
        description.put("numString", getNumTupleDatatypes().f2);
        description.put("tupleWidthOut", getNumTupleDatatypes().f0 + getNumTupleDatatypes().f1
                + getNumTupleDatatypes().f2 + 1); // add timestamp value manually
        return description;
    }

}
