package plangeneratorflink.utils;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.connection.ServerAddressHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class FlinkMetrics {

    final String restHostname;
    final String restProtocol = "http";
    final String restPort;
    final String restConnectionString;
    final Configuration config;
    int taskFailureCount = 0;
    MongoClient mongoClient;
    MongoDatabase db;

    public FlinkMetrics(Configuration config) {
        this.config = config;
        this.restHostname =
                config.get(ConfigOptions.key("hostname").stringType().defaultValue("localhost"));
        this.restPort =
                config.get(ConfigOptions.key("flinkRestApiPort").stringType().defaultValue("8081"));
        restConnectionString = restProtocol + "://" + restHostname + ":" + restPort;
        String mongoUsername =
                config.get(ConfigOptions.key("mongo.username").stringType().noDefaultValue());
        String mongoPassword =
                config.get(ConfigOptions.key("mongo.password").stringType().noDefaultValue());
        String mongoDatabase =
                config.get(ConfigOptions.key("mongo.database").stringType().noDefaultValue());

        ServerAddress serverAddress =
                ServerAddressHelper.createServerAddress(
                        config.get(
                                ConfigOptions.key("mongo.address")
                                        .stringType()
                                        .defaultValue("localhost")),
                        config.get(ConfigOptions.key("mongo.port").intType().defaultValue(27017)));
        MongoCredential mongoCredentials =
                MongoCredential.createCredential(
                        mongoUsername, mongoDatabase, mongoPassword.toCharArray());
        MongoClientOptions mongoOptions = MongoClientOptions.builder().build();
        mongoClient = new MongoClient(serverAddress, mongoCredentials, mongoOptions);
        db = mongoClient.getDatabase(mongoDatabase);
    }

    private JSONArray getRunningJobs() {
        JSONArray jsonArray = getJobs();
        jsonArray.removeIf(job -> !((JSONObject) job).get("status").equals("RUNNING"));
        return jsonArray;
    }

    private JSONArray getJobs() {
        JSONArray jsonArray = (JSONArray) getMetric(restConnectionString + "/jobs").get("jobs");
        return jsonArray;
    }

    private String getJobState(String jobId) {
        return (String) getMetric(restConnectionString + "/jobs/" + jobId).get("state");
    }

    private ArrayList<String> getJobIDsFromJobs(JSONArray jobs) {
        ArrayList<String> res = new ArrayList<>();
        jobs.forEach(job -> res.add(((JSONObject) job).get("id").toString()));
        return res;
    }

    private JSONArray getJobVertices(String jobID) {
        return (JSONArray) getMetric(restConnectionString + "/jobs/" + jobID).get("vertices");
    }

    private ArrayList<String> getVertexIDsFromJob(JSONArray vertices) {
        ArrayList<String> res = new ArrayList<>();
        vertices.forEach(vertex -> res.add(((JSONObject) vertex).get("id").toString()));
        return res;
    }

    private JSONArray getSubtasksOfVertex(String jobID, String vertexID) {
        return (JSONArray)
                getMetric(restConnectionString + "/jobs/" + jobID + "/vertices/" + vertexID)
                        .get("subtasks");
    }

    private ArrayList<String> getTaskManagerIDsOfVertex(JSONArray subtasksOfVertex) {
        ArrayList<String> res = new ArrayList<>();
        subtasksOfVertex.forEach(
                subtask -> res.add(((JSONObject) subtask).get("taskmanager-id").toString()));
        return res;
    }

    private JSONObject getMetric(String urlString) {
        return getMetric(urlString, "GET");
    }

    private JSONObject getMetric(String urlString, String requestMethod) {
        try {
            HttpClient httpClient = HttpClient.newHttpClient();
            HttpRequest request =
                    HttpRequest.newBuilder()
                            .uri(URI.create(urlString))
                            .method(requestMethod, HttpRequest.BodyPublishers.noBody())
                            .header("Content-Type", "application/json")
                            .build();
            HttpResponse<String> httpResponse =
                    httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            String result = httpResponse.body();

            JSONParser jsonParser = new JSONParser();
            JSONObject jsonResult = (JSONObject) jsonParser.parse(result);
            return jsonResult;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public boolean checkIfJobIsStillRunning() {
        int numberOfRunningJobs = getRunningJobs().size();
        if (numberOfRunningJobs == 0) {
            taskFailureCount++;
            if (taskFailureCount >= 300) {
                throw new IllegalStateException("job has ended prematurely multiple times.");
            }
            System.err.println(
                    "Job has ended prematurely. Try to move on (task failure count is: "
                            + taskFailureCount
                            + ")");
            return false;
        } else if (numberOfRunningJobs > 1) {
            throw new IllegalStateException("there is more than one job running at the same time");
        }
        taskFailureCount = 0;
        return true;
    }

//    public void logPlacement() {
//        ArrayList<String> jobIDsFromJobs = getJobIDsFromJobs(getRunningJobs());
//        System.out.println("jobIDsFromJobs: " + jobIDsFromJobs);
//        ArrayList<String> vertexIDsFromJob =
//                getVertexIDsFromJob(getJobVertices(jobIDsFromJobs.get(0)));
//        System.out.println("vertexIDSFromJob: " + vertexIDsFromJob);
//        for (String vertexID : vertexIDsFromJob) {
//            JSONArray subtasksOfVertex = getSubtasksOfVertex(jobIDsFromJobs.get(0), vertexID);
//            ArrayList<String> taskManagerIDsOfVertex = getTaskManagerIDsOfVertex(subtasksOfVertex);
//            // System.out.println("job: " + jobIDsFromJobs.get(0) + " | vertexID: " + vertexID +  "
//            // | taskManagerIDsOfVertex: " + taskManagerIDsOfVertex + " | subtasksOfVertex: " +
//            // subtasksOfVertex);
//            Map<String, Object> label = new HashMap<>();
//            taskManagerIDsOfVertex.forEach(
//                    taskManagerIdOfVertex -> {
//                        label.clear();
//                        label.put("component", vertexID);
//                        label.put("host", taskManagerIdOfVertex);
//                        JSONObject json = new JSONObject();
//                        json.putAll(label);
//                        String mongoCollectionPlacementName =
//                                (String)
//                                        config.get(
//                                                ConfigOptions.key("mongo.collection.placement")
//                                                        .stringType()
//                                                        .defaultValue("query_placement"));
//                        MongoCollection<JSONObject> collection =
//                                db.getCollection(mongoCollectionPlacementName, JSONObject.class);
//                        collection.insertOne(json);
//                    });
//        }
//    }

    public void cancelAllRunningJobs(boolean waitUntilJobsAreCanceled) {
        JSONArray runningJobs = getRunningJobs();
        runningJobs.forEach(
                job -> {
                    String jobID = (String) ((JSONObject) job).get("id");
                    getMetric(restConnectionString + "/jobs/" + jobID, "PATCH");
                });
        if (waitUntilJobsAreCanceled) {
            AtomicBoolean allJobsAreCanceled = new AtomicBoolean(true);
            ArrayList<String> jobIDsFromRunningJobs = getJobIDsFromJobs(runningJobs);
            do {
                try {
                    Thread.sleep(3 * 1000); // sleep for 5s
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                allJobsAreCanceled.set(true);
                jobIDsFromRunningJobs.forEach(
                        jobId -> {
                            String jobState = getJobState(jobId);
                            if (jobState.equals("FAILED")) {
                                throw new RuntimeException("a running job has failed.");
                            }
                            if (!jobState.equals("CANCELED")) {
                                System.out.println("Wait for successful job cancellation.");
                                allJobsAreCanceled.set(false);
                            }
                        });
            } while (!allJobsAreCanceled.get());
        }
    }

    public boolean checkTaskManagerAvailability(Boolean waitForAvailability) throws IOException {
        JSONArray taskManagers =
                (JSONArray) getMetric(restConnectionString + "/taskmanagers").get("taskmanagers");
        if (waitForAvailability) {
            if (taskManagers.size() == 0) {
                System.out.println(
                        "No task manager is running. Take care of that and press enter to continue.");
                System.in.read();
            }
        }
        if (taskManagers.size() > 0) {
            return true;
        }
        return false;
    }

//    public void logGrouping(StreamGraph streamGraph) {
//        //        Logger logger = LoggerFactory.getLogger("grouping");
//        JobGraph jobGraph = streamGraph.getJobGraph();
//        List<JobVertex> verticesSortedTopologicallyFromSources =
//                jobGraph.getVerticesSortedTopologicallyFromSources();
//        jobGraph.getVerticesSortedTopologicallyFromSources()
//                .forEach(
//                        jobVertex -> {
//                            String jobVertexName = jobVertex.getName();
//                            String[] split = jobVertexName.split(" -> ");
//                            Map<String, Object> label = new HashMap<>();
//                            Arrays.stream(split)
//                                    .forEach(
//                                            s -> {
//                                                if (s.startsWith("Source: ")) {
//                                                    s = s.substring(8);
//                                                } else if (s.startsWith("Sink: ")) {
//                                                    s = s.substring(6);
//                                                }
//                                                label.put(
//                                                        "component", jobVertex.getID().toString());
//                                                label.put("operator", s);
//                                                JSONObject json = new JSONObject();
//                                                json.putAll(label);
//
//                                                String mongoCollectionGroupingName =
//                                                        (String)
//                                                                config.get(
//                                                                        ConfigOptions.key(
//                                                                                        "mongo.collection.grouping")
//                                                                                .stringType()
//                                                                                .defaultValue(
//                                                                                        "query_grouping"));
//                                                MongoCollection<JSONObject> collection =
//                                                        db.getCollection(
//                                                                mongoCollectionGroupingName,
//                                                                JSONObject.class);
//                                                collection.insertOne(json);
//                                            });
//                        });
//    }
}
