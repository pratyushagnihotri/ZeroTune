package plangeneratorflink.operators.aggregate;

import org.apache.flink.api.common.functions.AggregateFunction;
import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class AggregateOperator extends AbstractOperator<AbstractAggregateFunction>
        implements AggregateFunction<DataTuple, ArrayList<DataTuple>, DataTuple> {
    private String keyByClass;
    private final String aggFunction;
    public LinkedHashMap<Class<?>, ArrayList<Object>> tupleContent = new LinkedHashMap<>();
    private HashMap<String, Object> windowDescription;

    public AggregateOperator(
            AbstractAggregateFunction function, String aggFunctionName, Class<?> klass) {
        this.function = function;
        this.aggFunction = aggFunctionName;
        this.klass = klass;
    }

    public void setKeyByClass(String keyByClass) {
        this.keyByClass = keyByClass;
    }

    public String getKeyByClass() {
        return this.keyByClass;
    }

    @Override
    public ArrayList<DataTuple> add(DataTuple element, ArrayList<DataTuple> acc) {
        acc.add(element);
        return acc;
    }

    @Override
    public ArrayList<DataTuple> createAccumulator() {
        return new ArrayList<>();
    }

    @Override
    public DataTuple getResult(ArrayList<DataTuple> acc) {
        throw new RuntimeException(
                "cannot use AggregateOperator class. Use specific functions (and use .getFunction() in aggregate() )");
    }

    @Override
    public ArrayList<DataTuple> merge(ArrayList<DataTuple> acc1, ArrayList<DataTuple> acc2) {
        acc1.addAll(acc2);
        return acc1;
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put(Constants.Features.aggFunction.name(), aggFunction);
        description.put(Constants.Features.aggClass.name(), klass.getSimpleName());
        description.put(Constants.Features.keyByClass.name(), keyByClass);
        if (windowDescription != null) {
            description.putAll(windowDescription);
            description.put(Constants.Features.operatorType.name(), "WindowedAggregateOperator");
        }
        return description;
    }

    public void addWindowDescription(HashMap<String, Object> description) {
        description.remove(Constants.Features.operatorType.name());
        description.remove(Constants.Features.id.name());
        windowDescription = description;
    }

    public String getAggFunction() {
        return aggFunction;
    }
}
