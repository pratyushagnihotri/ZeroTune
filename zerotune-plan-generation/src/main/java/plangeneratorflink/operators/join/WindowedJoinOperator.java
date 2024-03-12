package plangeneratorflink.operators.join;

import org.apache.flink.api.common.functions.JoinFunction;
import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class WindowedJoinOperator extends AbstractOperator<JoinFunction<DataTuple, DataTuple, DataTuple>> {
    private final HashMap<String, Object> windowDescr;

    public WindowedJoinOperator(String index, HashMap<String, Object> windowDescription) {
        super();
        this.id = index;
        this.windowDescr = windowDescription;
        this.function = getJoinFunction();
    }

    private JoinFunction<DataTuple, DataTuple, DataTuple> getJoinFunction() {
        return (dt1, dt2) -> {
            long timestamp =
                    Math.min(Long.parseLong(dt1.getTimestamp()), Long.parseLong(dt2.getTimestamp()));
            LinkedHashMap<Class<?>, ArrayList<Object>> content1 = dt1.getTupleContent();
            LinkedHashMap<Class<?>, ArrayList<Object>> content2 = dt2.getTupleContent();
            for (Map.Entry<Class<?>, ArrayList<Object>> entry : content1.entrySet()) {
                ArrayList<Object> combinedList = content1.get(entry.getKey());
                combinedList.addAll(content2.get(entry.getKey()));
                content1.put(entry.getKey(), combinedList);
            }
            return new DataTuple(content1, String.valueOf(timestamp));
        };
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        windowDescr.remove(Constants.Features.id.name());
        windowDescr.remove(Constants.Features.parallelism.name());
        description.putAll(windowDescr);
        description.put(Constants.Features.operatorType.name(), this.getClass().getSimpleName());
        return description;
    }

    public void setJoinKeyClass(Class joinBasedClass) {
        this.windowDescr.put(Constants.Features.joinKeyClass.name(), joinBasedClass.getSimpleName());
    }
}
