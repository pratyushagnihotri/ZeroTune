package plangeneratorflink.operators.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;
import plangeneratorflink.utils.RanGen;

import java.util.HashMap;

public class SyntheticSourceOperator extends AbstractOperator<DataGeneratorSource<DataTuple>>
        implements DataGenerator<DataTuple> {
    private Tuple3<Integer, Integer, Integer> numTupleDatatypes; // int, double, string

    private int rowsPerSecond;
    private DataGeneratorSource<DataTuple> dataGeneratorSource;

    public SyntheticSourceOperator(String id, Integer rowsPerSecond) {
        init(id, rowsPerSecond, null);
    }

    public SyntheticSourceOperator(String id, Integer rowsPerSecond, Integer tupleWidth) {
        init(id, rowsPerSecond, tupleWidth, tupleWidth, tupleWidth);
    }

    public SyntheticSourceOperator(
            String id,
            Integer rowsPerSecond,
            Integer tupleWidthInteger,
            Integer tupleWidthDouble,
            Integer tupleWidthString) {
        init(id, rowsPerSecond, tupleWidthInteger, tupleWidthDouble, tupleWidthString);
    }

    private void init(String id, Integer rowsPerSecond, Integer tupleWidth) {
        init(id, rowsPerSecond, tupleWidth, tupleWidth, tupleWidth);
    }

    private void init(
            String id,
            Integer rowsPerSecond,
            Integer tupleWidthInteger,
            Integer tupleWidthDouble,
            Integer tupleWidthString) {
        setId(id);
        if (rowsPerSecond == null) {
            rowsPerSecond = RanGen.randIntFromList(Constants.Synthetic.Train.EVENT_RATES);
        }
        if (tupleWidthInteger == null || tupleWidthDouble == null || tupleWidthString == null) {
            this.numTupleDatatypes =
                    new Tuple3<>(
                            RanGen.randIntRange(Constants.Synthetic.Train.INTEGER_RANGE),
                            RanGen.randIntRange(Constants.Synthetic.Train.DOUBLE_RANGE),
                            RanGen.randIntRange(Constants.Synthetic.Train.STRING_RANGE));
        } else {
            this.numTupleDatatypes =
                    new Tuple3<>(tupleWidthInteger, tupleWidthDouble, tupleWidthString);
        }
        this.rowsPerSecond = rowsPerSecond;
        dataGeneratorSource =
                new DataGeneratorSource<>(this, rowsPerSecond, null, getDescription());
        this.function = dataGeneratorSource;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public DataTuple next() {
        return new DataTuple(this.numTupleDatatypes);
    }

    @Override
    public void open(String arg0, FunctionInitializationContext arg1, RuntimeContext arg2)
            throws Exception {}

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
        description.put(
                "tupleWidthOut",
                getNumTupleDatatypes().f0
                        + getNumTupleDatatypes().f1
                        + getNumTupleDatatypes().f2
                        + 1); // add timestamp value manually
        return description;
    }
}
