package plangeneratorflink.operators.source;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;

import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.querybuilder.smartgrid.SmartPlugGenerator;
import plangeneratorflink.utils.DataTuple;

import java.util.ArrayList;
import java.util.HashMap;

public class SmartGridSourceOperator extends AbstractOperator<DataGeneratorSource<DataTuple>>
        implements DataGenerator<DataTuple> {
    private final SmartPlugGenerator generator = new SmartPlugGenerator();
    private int rowsPerSecond;
    private DataGeneratorSource<DataTuple> dataGeneratorSource;
    private ArrayList<String> deviceIds;

    public SmartGridSourceOperator(String id, int rowsPerSecond) {
        setId(id);
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
        DataTuple dt = generator.generate();
        return dt;
    }

    @Override
    public void open(
            String name, FunctionInitializationContext context, RuntimeContext runtimeContext)
            throws Exception {
        generator.initialize();
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put("operatorType", "SourceOperator");
        description.put("confEventRate", this.rowsPerSecond);
        description.put("numInteger", 5);
        description.put("numDouble", 1);
        description.put("numString", 1);
        description.put("tupleWidthOut", 5 + 1 + 1 + 1); // add timestamp value manually
        return description;
    }
}
