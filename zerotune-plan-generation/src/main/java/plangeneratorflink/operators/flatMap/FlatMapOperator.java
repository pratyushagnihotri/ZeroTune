package plangeneratorflink.operators.flatMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import plangeneratorflink.operators.AbstractOperator;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.DataTuple;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;

public class FlatMapOperator<T> extends AbstractOperator<FlatMapFunction<T, DataTuple>> {

    public FlatMapOperator(String index, FlatMapFunction<T, DataTuple> flatMapFunction) {
        super();
        this.id = index;
        this.function = flatMapFunction;
    }

    private static void flatMap(String sensorEvent, Collector<DataTuple> out) {
        String[] s = sensorEvent.split(" ");
        if (s.length != 8) {
            return;
        }
        LinkedHashMap<Class<?>, ArrayList<Object>> dataTupleContent = new LinkedHashMap<>();
        ArrayList<Object> stringContent = new ArrayList<>();
        ArrayList<Object> doubleContent = new ArrayList<>();
        ArrayList<Object> intContent = new ArrayList<>();
        long timestamp;
        try {
            timestamp = LocalDateTime.parse(s[0] + " " + s[1].split("\\.")[0], DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).toEpochSecond(ZoneOffset.UTC);
            intContent.add(Integer.parseInt(s[2]));            // epoch
            intContent.add(Integer.parseInt(s[3]));            // moteid
            doubleContent.add(Double.parseDouble(s[4]));       // temperature
            doubleContent.add(Double.parseDouble(s[5]));       // humidity
            doubleContent.add(Double.parseDouble(s[6]));       // light
            doubleContent.add(Double.parseDouble(s[7]));       // voltage
        } catch (Exception e) {
            System.err.println("Wrong format --> Error parsing: " + sensorEvent);
            return;
        }


        dataTupleContent.put(String.class, stringContent);
        dataTupleContent.put(Double.class, doubleContent);
        dataTupleContent.put(Integer.class, intContent);
        out.collect(new DataTuple(dataTupleContent, String.valueOf(timestamp)));
    }

    @Override
    public HashMap<String, Object> getDescription() {
        HashMap<String, Object> description = super.getDescription();
        description.put(Constants.Features.operatorType.name(), this.getClass().getSimpleName());
        return description;
    }
}
