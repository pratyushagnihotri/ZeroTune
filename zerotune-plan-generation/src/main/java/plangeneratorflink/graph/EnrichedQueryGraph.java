package plangeneratorflink.graph;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.graphstream.graph.Graph;

public class EnrichedQueryGraph extends Tuple3<StreamGraph, Graph, String> {
    public EnrichedQueryGraph(StreamGraph topo, Graph graph, String name) {
        super(topo, graph, name);
    }
    public StreamGraph getTopo(){
        return this.f0;
    }

    public Graph getGraph(){
        return this.f1;
    }

    public String getName(){
        return this.f2;
    }
}
