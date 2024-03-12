package plangeneratorflink.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Comparator;
import java.util.HashMap;
import java.util.stream.Collectors;

public class SearchHeuristicCosts {
    private final String queryId;
    private Double dryPredLatency;
    private Double dryPredTp;
    private Double wetPredLatency;
    private Double wetPredTp;
    private Double actLatency;
    private Double actTp;
    private Double dryQErrorLatency;
    private Double dryQErrorTp;
    private Double dryPredOverallCosts;
    private Double wetQErrorLatency;
    private Double wetQErrorTp;
    private Double wetPredOverallCosts;
    private Double actOverallCosts;
    private HashMap<String, Tuple2<String, Integer>> parallelismSet;


    public SearchHeuristicCosts(String queryId) {
        this.queryId = queryId;
    }

    public String getQueryId() {
        return queryId;
    }

    public Double getDryPredLatency() {
        return dryPredLatency;
    }

    public void setDryPredLatency(Double dryPredLatency) {
        this.dryPredLatency = dryPredLatency;
    }

    public Double getDryPredTp() {
        return dryPredTp;
    }

    public void setDryPredTp(Double dryPredTp) {
        this.dryPredTp = dryPredTp;
    }

    public Double getActLatency() {
        return actLatency;
    }

    public void setActLatency(Double actLatency) {
        this.actLatency = actLatency;
    }

    public Double getActTp() {
        return actTp;
    }

    public void setActTp(Double actTp) {
        this.actTp = actTp;
    }

    public Double getDryQErrorLatency() {
        return dryQErrorLatency;
    }

    public void setDryQErrorLatency(Double dryQErrorLatency) {
        this.dryQErrorLatency = dryQErrorLatency;
    }

    public Double getDryQErrorTp() {
        return dryQErrorTp;
    }

    public void setDryQErrorTp(Double dryQErrorTp) {
        this.dryQErrorTp = dryQErrorTp;
    }

    public Double getDryPredOverallCosts() {
        return dryPredOverallCosts;
    }

    public void setDryPredOverallCosts(Double dryPredOverallCosts) {
        this.dryPredOverallCosts = dryPredOverallCosts;
    }

    public Double getActOverallCosts() {
        return actOverallCosts;
    }

    public void setActOverallCosts(Double actOverallCosts) {
        this.actOverallCosts = actOverallCosts;
    }

    public Double getWetPredLatency() {
        return wetPredLatency;
    }

    public void setWetPredLatency(Double wetPredLatency) {
        this.wetPredLatency = wetPredLatency;
    }

    public Double getWetPredTp() {
        return wetPredTp;
    }

    public void setWetPredTp(Double wetPredTp) {
        this.wetPredTp = wetPredTp;
    }

    public Double getWetQErrorLatency() {
        return wetQErrorLatency;
    }

    public void setWetQErrorLatency(Double wetQErrorLatency) {
        this.wetQErrorLatency = wetQErrorLatency;
    }

    public Double getWetQErrorTp() {
        return wetQErrorTp;
    }

    public void setWetQErrorTp(Double wetQErrorTp) {
        this.wetQErrorTp = wetQErrorTp;
    }

    public Double getWetPredOverallCosts() {
        return wetPredOverallCosts;
    }

    public void setWetPredOverallCosts(Double wetPredOverallCosts) {
        this.wetPredOverallCosts = wetPredOverallCosts;
    }

    public String getFormattedParallelismSet() {
        StringBuilder sb = new StringBuilder();
        for (String operatorId :
                parallelismSet.keySet().stream()
                        .sorted(
                                Comparator.comparingInt(
                                        s -> {
                                            String[] operatorIdSplitted = s.split("-");
                                            return Integer.parseInt(
                                                    operatorIdSplitted[
                                                            operatorIdSplitted.length - 1]);
                                        }))
                        .collect(Collectors.toList())) {
            Tuple2<String, Integer> operatorAttributes = parallelismSet.get(operatorId);
            String[] operatorIdSplitted = operatorId.split("-");
            char operatorNameStart;
            if (operatorAttributes.f0.equals("WindowedAggregateOperator")) {
                operatorNameStart = 'A';
            } else if (operatorAttributes.f0.equals("WindowedJoinOperator")) {
                operatorNameStart = 'J';
            } else {
                operatorNameStart = operatorAttributes.f0.charAt(0);
            }
            sb.append("(");
            sb.append(operatorNameStart);
            sb.append(operatorIdSplitted[operatorIdSplitted.length - 1]);
            sb.append("->P");
            sb.append(operatorAttributes.f1);
            sb.append(") ");
        }
        if (sb.length() > 2) {
            sb.setLength(sb.length() - 1);
        }
        return sb.toString();
    }

    public HashMap<String, Tuple2<String, Integer>> getParallelismSet() {
        return parallelismSet;
    }

    public void setParallelismSet(HashMap<String, Tuple2<String, Integer>> parallelismSet) {
        this.parallelismSet = parallelismSet;
    }
}
