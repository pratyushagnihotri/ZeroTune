package plangeneratorflink.operators.sink;

import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.internal.connection.ServerAddressHelper;
import org.json.simple.JSONObject;
import plangeneratorflink.utils.DataTuple;

import java.util.ArrayList;
import java.util.HashMap;

public class SinkOperatorFunction extends RichSinkFunction<DataTuple> {

    private final String queryName;
    private final ArrayList<Long> latencies = new ArrayList<>();
    private Configuration config;
    private boolean initialized = false;
    private long startTime;
    private int inputCounter;
    private long lastEntryTime;
    private boolean done = false;
    //    private Logger logger;

    public SinkOperatorFunction(String queryName, Configuration config) {
        this.queryName = queryName;
        this.config = config;
    }

    @Override
    public void invoke(DataTuple value, Context context) throws Exception {
        super.invoke(value, context);
        if (!initialized) {
            this.initialized = true;
            this.startTime = System.nanoTime();
        }
        if (!done) {
            this.inputCounter++;
            String timestamp;
            timestamp = value.getTimestamp();
            long latency = System.currentTimeMillis() - Long.parseLong(timestamp);
            latencies.add(latency);
            this.lastEntryTime = System.nanoTime();
            // end measurements after 30s of incoming data (same as in Flink-Observation
            if (this.lastEntryTime - this.startTime > 30 * 1e9) {
                done = true;
            }
        }
    }

    @Override
    public void close() throws Exception {
        // ToDo: where is labels logging used?
        // ToDo: call close() on cancel query call
        super.close();
        System.out.println("close() in PlanGeneratorSink called");
        // SinkFunction.super.finish();
        long elapsedTime = this.lastEntryTime - this.startTime;
        double seconds = (double) elapsedTime / 1_000_000_000L;

        HashMap<String, Object> label = new HashMap<>();
        if (this.inputCounter == 0) {
            label.put("throughput", "null");
            label.put("latency", "null");
            label.put("counter", "null");
            label.put("duration", "null");
        } else {
            double meanTp = (double) this.inputCounter / seconds;
            double meanLat = this.latencies.stream().mapToDouble(val -> val).average().orElse(0.0);
            // if (meanTp > 10 *
            // Arrays.stream(Constants.Synthetic.Train.EVENT_RATES).max().getAsInt()) {
            label.put("throughput", meanTp);
            label.put("latency", meanLat);
            label.put("counter", this.inputCounter);
            label.put("duration", seconds);
        }
        label.put("id", this.queryName);
        JSONObject json = new JSONObject();
        json.putAll(label);

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
        MongoClient mongoClient = new MongoClient(serverAddress, mongoCredentials, mongoOptions);
        MongoDatabase db = mongoClient.getDatabase(mongoDatabase);
        String mongoCollectionLabelsName =
                (String)
                        config.get(
                                ConfigOptions.key("mongo.collection.labels")
                                        .stringType()
                                        .defaultValue("query_labels"));
        MongoCollection<JSONObject> collection =
                db.getCollection(mongoCollectionLabelsName, JSONObject.class);
        collection.insertOne(json);
        mongoClient.close();
    }
}
