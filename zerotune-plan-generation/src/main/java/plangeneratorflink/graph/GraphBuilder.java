package plangeneratorflink.graph;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.AtomicDouble;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.connection.ServerAddressHelper;
import org.bson.Document;
import org.graphstream.graph.Edge;
import org.graphstream.graph.Graph;
import org.graphstream.graph.Node;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSinkDOT;
import plangeneratorflink.utils.Constants;
import plangeneratorflink.utils.SearchHeuristic;

import java.io.File;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

/** This class builds the enriched graph that is later used as training data. */
public class GraphBuilder {
    private final Configuration config;
    MongoDatabase db;
    private Graph currentGraph;
    private MongoClient mongoClient;
    private MongoCollection<Document> mongoCollection;
    private FileSinkDOT fs;
    private StringWriter writer;
    private TreeMap<String, Integer> componentValues = new TreeMap<>();
    private HashMap<String, HashMap<String, Object>> physicalNodes = new HashMap<>();

    public GraphBuilder(Configuration config) {
        this.config = config;
        this.currentGraph = null;
        this.fs = new FileSinkDOT();
        writer = new StringWriter();
        String mongoAddress =
                this.config.getString(
                        ConfigOptions.key("mongo.address").stringType().noDefaultValue());
        Integer mongoPort =
                this.config.getInteger(ConfigOptions.key("mongo.port").intType().noDefaultValue());
        String mongoUsername =
                this.config.get(ConfigOptions.key("mongo.username").stringType().noDefaultValue());
        String mongoPassword =
                this.config.get(ConfigOptions.key("mongo.password").stringType().noDefaultValue());
        String mongoDatabase =
                this.config.getString(
                        ConfigOptions.key("mongo.database").stringType().noDefaultValue());
        String mongoCollectionGraphsName =
                this.config.getString(
                        ConfigOptions.key("mongo.collection.graphs").stringType().noDefaultValue());

        ServerAddress serverAddress =
                ServerAddressHelper.createServerAddress(mongoAddress, mongoPort);
        MongoCredential mongoCredentials =
                MongoCredential.createCredential(
                        mongoUsername, mongoDatabase, mongoPassword.toCharArray());
        MongoClientOptions mongoOptions = MongoClientOptions.builder().build();
        mongoClient = new MongoClient(serverAddress, mongoCredentials, mongoOptions);
        db = mongoClient.getDatabase(mongoDatabase);
        mongoCollection = db.getCollection(mongoCollectionGraphsName);
    }

    /**
     * This method takes the current Graph object and merges it with the the information coming
     * from: 1. the Observations (selectivities and tuple widths) 2. The Grouping of single
     * operators (group ID) 3. The placement of the operators, (instance ID and instance size) In
     * case of local execution, a script is called in order to collect distributed logs
     *
     * @param query Name/ID of the query
     * @throws IOException
     */
    public void buildEnrichedQueryGraph(EnrichedQueryGraph query) throws IOException {
        currentGraph = query.getGraph();
        Graph fannedGraph = new SingleGraph(currentGraph.getId());
        componentValues = new TreeMap<>();
        physicalNodes = new HashMap<>();
        String lastNodeCreated = null;
        String logFolder =
                this.config.getString(ConfigOptions.key("logDir").stringType().defaultValue(""))
                        + "/graphs/";
        String logPath = logFolder + query.getName() + ".graph";
        //        String logFannedPath =
        //
        // this.config.getString(ConfigOptions.key("logDir").stringType().defaultValue(""))
        //                        + "/"
        //                        + query.getName()
        //                        + "-fanned"
        //                        + ".graph";

        String mongoCollectionObservationName =
                this.config.getString(
                        ConfigOptions.key("mongo.collection.observations")
                                .stringType()
                                .noDefaultValue());

        MongoCollection<Document> observationCollection =
                db.getCollection(mongoCollectionObservationName);
        BasicDBObject findObservationsCriteria = new BasicDBObject();
        // for every operator
        for (Iterator<Node> it = currentGraph.nodes().iterator(); it.hasNext(); ) {
            Node node = it.next();
            findObservationsCriteria.put("id", node.getId());
            // for every recorded operator-parallelism-instance (=observation)
            AtomicDouble avgRealSelectivitySum = new AtomicDouble(0.0);
            AtomicInteger avgRealSelectivityCount = new AtomicInteger(0);
            for (Document observation : observationCollection.find(findObservationsCriteria)) {
                // add new physical node and edges (function keeps care if it already exists)
                lastNodeCreated =
                        addPhysicalNodeInformationsToGraph(
                                currentGraph, observation, lastNodeCreated);
                HashMap<String, Object> instanceData = new HashMap<>();
                // for every attribute of the observation
                for (Map.Entry<String, Object> obsAtt : observation.entrySet()) {
                    // ignore the following attributes as they are already included in the
                    // physicalNode type and would be duplicates
                    if (obsAtt.getKey().equals("totalMemory")
                            || obsAtt.getKey().equals("maxCPUFreq")
                            || obsAtt.getKey().equals("networkLinkSpeed")
                            || obsAtt.getKey().equals("physicalCPUCores")) {
                        continue;
                    }
                    // set attribute if is not part of instances-attribute
                    if (!obsAtt.getKey().equals("_id")
                            && !obsAtt.getKey().equals("host")
                            && !obsAtt.getKey().equals("avgCPULoad")
                            && !obsAtt.getKey().equals("outputRate")
                            && !obsAtt.getKey().equals("inputRate")
                            && !obsAtt.getKey().equals("realSelectivity")) {
                        // if it is the component-attribute, convert it before adding it
                        if (obsAtt.getKey().equals("component")) {
                            node.setAttribute(
                                    obsAtt.getKey(),
                                    String.valueOf(
                                            getGeneralizedComponentValue(
                                                    (String) obsAtt.getValue())));
                        } else {
                            node.setAttribute(obsAtt.getKey(), String.valueOf(obsAtt.getValue()));
                        }
                        // add attributes about the parallelism-instance to the instanceData map
                    } else if (!obsAtt.getKey().equals("_id")) {
                        instanceData.put(obsAtt.getKey(), String.valueOf(obsAtt.getValue()));
                        if (obsAtt.getKey().equals("realSelectivity")) {
                            avgRealSelectivityCount.incrementAndGet();
                            avgRealSelectivitySum.addAndGet((Double) obsAtt.getValue());
                        }
                    }
                }
                ArrayList<HashMap<String, Object>> instances = new ArrayList<>();
                // if instances already exists grab it
                if (node.hasAttribute("instances")) {
                    instances = (ArrayList<HashMap<String, Object>>) node.getAttribute("instances");
                }
                // adding the newly recorded instanceData to the instances
                instances.add(instanceData);
                node.setAttribute("instances", instances);
            }
            if (avgRealSelectivityCount.get() > 0) {
                node.setAttribute(
                        "avgRealSelectivity",
                        String.valueOf(
                                avgRealSelectivitySum.get()
                                        / ((double) avgRealSelectivityCount.get())));
            }
        }
        // the logical graph exists, lets create a fanned out graph where every parallelism is a
        // separate node
        // fannedGraph = createFannedGraph(currentGraph, fannedGraph);
        System.out.println("logPath PlanGeneratorFlink: " + logPath);
        File directory = new File(logFolder);
        if (!directory.exists()) {
            directory.mkdir();
        }
        fs.writeAll(currentGraph, logPath);
        // fs.writeAll(fannedGraph, logFannedPath);
    }



    private String addPhysicalNodeInformationsToGraph(
            Graph currentGraph, Document observation, String lastNodeCreated) {
        // if observation includes host attribute and physicalNode doesn't exist so far in the graph
        if (observation.containsKey("host") && observation.containsKey("id")) {
            String host = (String) observation.get("host");
            String id = (String) observation.get("id");
            if (currentGraph.getNode(host) == null) {
                Node physicalNode = currentGraph.addNode(host);

                physicalNode.setAttribute("host", host);
                physicalNode.setAttribute("operatorType", "PhysicalNode");
                physicalNode.setAttribute(
                        "totalMemory", String.valueOf(observation.get("totalMemory")));
                physicalNode.setAttribute(
                        "maxCPUFreq", String.valueOf(observation.get("maxCPUFreq")));
                physicalNode.setAttribute(
                        "networkLinkSpeed", String.valueOf(observation.get("networkLinkSpeed")));
                physicalNode.setAttribute(
                        "physicalCPUCores", String.valueOf(observation.get("physicalCPUCores")));
                if (lastNodeCreated != null) {
                    currentGraph.addEdge(
                            lastNodeCreated + "-->" + host,
                            currentGraph.getNode(lastNodeCreated),
                            physicalNode);
                }
                lastNodeCreated = host;
            }
            Edge edge = currentGraph.getEdge((id + " -> " + host));
            if (edge == null) {
                edge = currentGraph.addEdge((id + " -> " + host), id, host);
            }
            Integer occurences = (Integer) edge.getAttribute("occurences");
            occurences = occurences != null ? occurences + 1 : 1;
            edge.setAttribute("occurences", occurences);
        }
        return lastNodeCreated;
    }

    // create out of the existing graph a new graph that has a node for every parallelism-instance
    // with its corresponding attributes and edges
    private Graph createFannedGraph(Graph currentGraph, Graph fannedGraph) {
        // for every existing node, create as many new nodes as the amount of parallelism is
        currentGraph
                .nodes()
                .forEach(
                        node -> {
                            Integer parallelism = node.getAttribute("parallelism", Integer.class);
                            ArrayList<HashMap<String, Object>> instances =
                                    node.getAttribute("instances", ArrayList.class);
                            for (int i = 0; i < parallelism; i++) {
                                HashMap<String, Object> instanceAttributes = new HashMap<>();
                                if (instances != null && instances.size() > i) {
                                    instanceAttributes = instances.get(i);
                                }
                                Node fannedNode = fannedGraph.addNode(node.getId() + "-" + i);
                                // copy the existing attributes to the new fanned node
                                node.attributeKeys()
                                        .forEach(
                                                key -> {
                                                    if (!key.equals("instances")
                                                            && !key.equals("id")) {
                                                        fannedNode.setAttribute(
                                                                key,
                                                                String.valueOf(
                                                                        node.getAttribute(key)));
                                                    }
                                                });
                                fannedNode.setAttribute("id", String.valueOf(fannedNode.getId()));
                                // add the instance attributes to the new node
                                instanceAttributes
                                        .entrySet()
                                        .forEach(
                                                instanceAttribute -> {
                                                    if (instanceAttribute
                                                                    .getKey()
                                                                    .equals("inputRate")
                                                            || instanceAttribute
                                                                    .getKey()
                                                                    .equals("outputRate")
                                                            || instanceAttribute
                                                                    .getKey()
                                                                    .equals("avgCPULoad")
                                                            || instanceAttribute
                                                                    .getKey()
                                                                    .equals("realSelectivity")) {
                                                        fannedNode.setAttribute(
                                                                instanceAttribute.getKey(),
                                                                String.valueOf(
                                                                        instanceAttribute
                                                                                .getValue()));
                                                    }
                                                    // create edge between
                                                    // operator-parallelism-instance and physical
                                                    // node
                                                    if (instanceAttribute.getKey().equals("host")) {
                                                        String host =
                                                                (String)
                                                                        instanceAttribute
                                                                                .getValue();
                                                        fannedGraph.addEdge(
                                                                fannedNode.getId() + "->" + host,
                                                                fannedNode,
                                                                fannedGraph.getNode(host));
                                                    }
                                                });
                            }
                        });

        // create edges
        AtomicInteger edgeIndex = new AtomicInteger();
        currentGraph
                .edges()
                .forEach(
                        edge -> {
                            Node sourceLogicalOperator = edge.getNode0();
                            Node endLogicalOperator = edge.getNode1();
                            fannedGraph
                                    .nodes()
                                    .filter(
                                            fannedSourceNode -> {
                                                if (fannedSourceNode
                                                        .getAttribute("operatorType", String.class)
                                                        .equals("physicalNode")) {
                                                    return false;
                                                }
                                                return fannedSourceNode
                                                        .getId()
                                                        .substring(
                                                                0,
                                                                fannedSourceNode
                                                                        .getId()
                                                                        .lastIndexOf("-"))
                                                        .equals(sourceLogicalOperator.getId());
                                            })
                                    .forEach(
                                            fannedSourceNode -> {
                                                fannedGraph
                                                        .nodes()
                                                        .filter(
                                                                fannedEndNode -> {
                                                                    if (fannedEndNode
                                                                            .getAttribute(
                                                                                    "operatorType",
                                                                                    String.class)
                                                                            .equals(
                                                                                    "physicalNode")) {
                                                                        return false;
                                                                    }
                                                                    return fannedEndNode
                                                                            .getId()
                                                                            .substring(
                                                                                    0,
                                                                                    fannedEndNode
                                                                                            .getId()
                                                                                            .lastIndexOf(
                                                                                                    "-"))
                                                                            .equals(
                                                                                    endLogicalOperator
                                                                                            .getId());
                                                                })
                                                        .forEach(
                                                                fannedEndNode -> {
                                                                    fannedGraph.addEdge(
                                                                            fannedGraph.getId()
                                                                                    + "-"
                                                                                    + edgeIndex
                                                                                            .getAndIncrement(),
                                                                            fannedSourceNode,
                                                                            fannedEndNode);
                                                                });
                                            });
                        });

        return fannedGraph;
    }

    // this function converts the uuid of the component (like ai4nklj3...) to an incrementing index
    // number (like 1, 2, ...)
    private Integer getGeneralizedComponentValue(String value) {
        return componentValues.computeIfAbsent(
                value,
                s -> {
                    if (componentValues.size() > 0) {
                        return componentValues.entrySet().stream()
                                        .max((t0, t1) -> t0.getValue() > t1.getValue() ? 1 : -1)
                                        .get()
                                        .getValue()
                                + 1;
                    } else {
                        return 0;
                    }
                });
    }

    public void closeMongoDBConnection() {
        mongoClient.close();
    }

    public void writeLabelsFile(String queryId) {

        String logPath =
                this.config.getString(ConfigOptions.key("logDir").stringType().noDefaultValue())
                        + "/"
                        + "query.labels";

        String mongoCollectionLabelsName =
                this.config.getString(
                        ConfigOptions.key("mongo.collection.labels").stringType().noDefaultValue());

        MongoCollection<Document> labels = db.getCollection(mongoCollectionLabelsName);
        BasicDBObject findLabels = new BasicDBObject();
        findLabels.put("id", queryId);
        MongoCursor<Document> labelIterator = labels.find(findLabels).iterator();
        if (labelIterator.hasNext()) {
            Document label = labelIterator.next();
            label.remove("_id");
            try {
                Files.writeString(Path.of(logPath), label.toJson() + "\n", CREATE, APPEND);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
