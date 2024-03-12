package plangeneratorflink.utils;

import org.apache.flink.api.java.tuple.Tuple2;

import org.graphstream.graph.Graph;
import org.graphstream.graph.implementations.SingleGraph;
import org.graphstream.stream.file.FileSource;
import org.graphstream.stream.file.FileSourceDGS;
import org.graphstream.stream.file.FileSourceDOT;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class EnvironmentExplorer {

    public int determineMaxPossibleParallelism() throws IOException, ParseException {
        // "kubectl get nodes -l pgf.type=tm --no-headers -o
        // custom-columns=':status.allocatable.cpu' | awk '{cpu+=$1} END
        // {print cpu}'");
        Process p =
                new ProcessBuilder(
                                "/bin/sh",
                                "-c",
                                "kubectl get nodes -l pgf.type=tm --no-headers -o custom-columns=':status.allocatable.cpu' | awk '{cpu+=$1-1} END {print cpu}'")
                        .start();
        String cpuCores = new String(p.getInputStream().readAllBytes());
        try {
            return Integer.parseInt(cpuCores.replace("\n", "").replace("\r", ""));
        } catch (NumberFormatException e) {
            System.err.println("Cannot get max. possible parallelism of cluster: " + e);
        }
        return 0;
    }

    public Tuple2<String, Integer> determineFlinkRestAPIAddressFromKubernetes()
            throws IOException, InterruptedException, ParseException {
        Process p =
                Runtime.getRuntime()
                        .exec(
                                "kubectl get services -n plangeneratorflink-namespace plangeneratorflink-cluster-rest -ojson");
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String s;
        String kubernetesRestServiceResultString = "";
        while ((s = br.readLine()) != null) {
            kubernetesRestServiceResultString += s;
        }
        String kubernetesRestServiceIP =
                (String)
                        ((JSONArray)
                                        ((JSONObject)
                                                        ((JSONObject)
                                                                        new JSONParser()
                                                                                .parse(
                                                                                        kubernetesRestServiceResultString))
                                                                .get("spec"))
                                                .get("clusterIPs"))
                                .get(0);
        int kubernetesRestServicePort =
                ((Long)
                                ((JSONObject)
                                                ((JSONArray)
                                                                ((JSONObject)
                                                                                ((JSONObject)
                                                                                                new JSONParser()
                                                                                                        .parse(
                                                                                                                kubernetesRestServiceResultString))
                                                                                        .get(
                                                                                                "spec"))
                                                                        .get("ports"))
                                                        .get(0))
                                        .get("targetPort"))
                        .intValue();
        p.waitFor();
        if (p.exitValue() != 0) {
            throw new RuntimeException(
                    "cannot get kubernetes rest service address. Returncode: "
                            + p.exitValue()
                            + " | Received IP: "
                            + kubernetesRestServiceIP
                            + " | Received port: "
                            + kubernetesRestServicePort);
        }
        p.destroy();

        return new Tuple2<>(kubernetesRestServiceIP, kubernetesRestServicePort);
    }

    public Tuple2<String, Integer> determineMongoDBAddressFromKubernetes()
            throws IOException, InterruptedException, ParseException {
        Process p =
                Runtime.getRuntime()
                        .exec(
                                "kubectl get services -n plangeneratorflink-namespace mongodb -ojson");
        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String s;
        String mongoDBServiceResultString = "";
        while ((s = br.readLine()) != null) {
            mongoDBServiceResultString += s;
        }
        if (mongoDBServiceResultString.length() <= 0) {
            throw new RuntimeException(
                    "Cannot determine MongoDB IP address for distributed logging.");
        }
        String kubernetesMongoDBServiceIP =
                (String)
                        ((JSONArray)
                                        ((JSONObject)
                                                        ((JSONObject)
                                                                        new JSONParser()
                                                                                .parse(
                                                                                        mongoDBServiceResultString))
                                                                .get("spec"))
                                                .get("clusterIPs"))
                                .get(0);
        int kubernetesMongoDBServicePort =
                ((Long)
                                ((JSONObject)
                                                ((JSONArray)
                                                                ((JSONObject)
                                                                                ((JSONObject)
                                                                                                new JSONParser()
                                                                                                        .parse(
                                                                                                                mongoDBServiceResultString))
                                                                                        .get(
                                                                                                "spec"))
                                                                        .get("ports"))
                                                        .get(0))
                                        .get("targetPort"))
                        .intValue();
        p.waitFor();
        if (p.exitValue() != 0) {
            throw new RuntimeException(
                    "cannot get kubernetes mongoDB service address. Returncode: "
                            + p.exitValue()
                            + " | Received IP: "
                            + kubernetesMongoDBServiceIP
                            + " | Received port: "
                            + kubernetesMongoDBServicePort);
        }
        p.destroy();

        return new Tuple2<>(kubernetesMongoDBServiceIP, kubernetesMongoDBServicePort);
    }

    public Tuple2<String, Integer> determineMongoDBAddressFromDocker()
            throws IOException, InterruptedException {
        return new Tuple2<>("localhost", 27017);
        //        String mongoDockerContainerName = "mongodblocal_mongo_1";
        //        Process p =
        //                Runtime.getRuntime()
        //                        .exec(
        //                                "docker inspect -f
        // '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "
        //                                        + mongoDockerContainerName);
        //        BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()));
        //        String s;
        //        String mongoDBDockerContainerIP = "";
        //        while ((s = br.readLine()) != null) {
        //            mongoDBDockerContainerIP += s;
        //        }
        //        if (mongoDBDockerContainerIP.length() <= 0) {
        //            throw new RuntimeException(
        //                    "Cannot determine MongoDB IP address for distributed logging.");
        //        }
        //        mongoDBDockerContainerIP =
        //                mongoDBDockerContainerIP.substring(1, mongoDBDockerContainerIP.length() -
        // 1);
        //        p.waitFor();
        //        if (p.exitValue() != 0) {
        //            throw new RuntimeException(
        //                    "cannot get docker mongoDB service address. Returncode: "
        //                            + p.exitValue()
        //                            + " | Received IP: "
        //                            + mongoDBDockerContainerIP);
        //        }
        //        p.destroy();

        //        return new Tuple2<>(mongoDBDockerContainerIP, 27017);
    }
}
