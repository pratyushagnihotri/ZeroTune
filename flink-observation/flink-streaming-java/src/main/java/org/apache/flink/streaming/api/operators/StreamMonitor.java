/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.operators.translation.WrappingFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.runtime.operators.util.StreamMonitorMongoClient;
import org.apache.flink.streaming.runtime.operators.windowing.WindowOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.json.simple.JSONObject;
import oshi.SystemInfo;
import oshi.hardware.CentralProcessor;
import oshi.hardware.HardwareAbstractionLayer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * Flink-Observation: This class is entirely new and added to the storm sources. The StreamMonitor
 * is attached to the processor nodes that are used in the Stream API. It is called for every
 * incoming and outgoing event and keeps track of the data characteristics (like tuple width,
 * selectivities, etc.) As no shutdown hooks can be reveiced here when shutting down the topology,
 * the measurement has a defined length It starts when the first tuple arrives. At the end, the data
 * characteristics are written into the logs per operator.
 */
public class StreamMonitor<T> implements Serializable {

    private static final long serialVersionUID = 1L;

    private final HashMap<String, Object> description;
    private final long duration = 30_000_000_000L; // 30 seconds, starting after first call
    private final T operator;
    private final ArrayList<Integer> windowLengths;
    private final boolean disableStreamMonitor;
    private WindowOperator windowOperator = null;
    private boolean initialized;
    private boolean outputInitialized;
    private boolean observationMade;
    private long startTime;
    private int inputCounter;
    private int outputCounter;
    private boolean localMode;
    private int joinSize1 = 0;
    private int joinSize2 = 0;
    private int joinPartners = 0;
    private int joinInputWidthLeftSide = -1;
    private int joinInputWidthRightSide = -1;
    private int tupleWidthIn = -1;

    private long[] prevTicks;
    private ExecutionConfig config;

    public StreamMonitor(HashMap<String, Object> description, T operator) {
        if (description == null) {
            description = new HashMap<>();
        }
        this.description = description;
        this.disableStreamMonitor = false;
        //        this.description.put("tupleWidthIn", -1);
        //        this.description.put("tupleWidthOut", -1);
        this.initialized = false;
        this.outputInitialized = false;
        this.observationMade = false;
        this.operator = operator;
        this.windowLengths = new ArrayList<>();
        if (this.operator instanceof StreamFilter
                || this.operator instanceof WindowOperator
                || this.operator instanceof WrappingFunction) {
            this.description.put("realSelectivity", 0.0);
        }
    }

    public <T> void reportInput(T input, ExecutionConfig config) {
        try {
            if (this.disableStreamMonitor) {
                return;
            }
            // if this operator is a join operator and left or right side input width isn't
            // already set
            if (this.operator instanceof WrappingFunction
                    && (this.joinInputWidthLeftSide == -1 || this.joinInputWidthRightSide == -1)) {
                // store left or right input width of join
                CoGroupedStreams.TaggedUnion<Tuple, Tuple> unitedTuple =
                        (CoGroupedStreams.TaggedUnion<Tuple, Tuple>) input;
                if (this.joinInputWidthLeftSide == -1 && unitedTuple.isOne()) {
                    this.joinInputWidthLeftSide = getTupleSize(input);
                } else if (this.joinInputWidthRightSide == -1) {
                    this.joinInputWidthRightSide = getTupleSize(input);
                }
            }
            if (!initialized) {
                initialized = true;
                this.config = config;
                this.startTime = System.nanoTime();
                tupleWidthIn = getTupleSize(input);
                prevTicks = new long[CentralProcessor.TickType.values().length];
            }

            this.inputCounter++;
            checkIfObservationEnd();

        } catch (Exception e) {
            System.err.println(
                    "error while processing reportInput() in StreamMonitor. Error: "
                            + e.getMessage());
        }
    }

    public <T> void reportOutput(T output) {
        try {
            if (this.disableStreamMonitor) {
                return;
            }
            if (!this.outputInitialized) {
                this.description.put("tupleWidthOut", getTupleSize(output));
                this.outputInitialized = true;
            }
            this.outputCounter++;
            checkIfObservationEnd();
        } catch (Exception e) {
            System.err.println(
                    "error while processing reportInput() in StreamMonitor. Error: "
                            + e.getMessage());
        }
    }

    public void reportJoinSelectivity(int size1, int size2, int joinPartners) {
        try {
            if (this.disableStreamMonitor) {
                return;
            }
            this.joinSize1 += size1;
            this.joinSize2 += size2;
            this.joinPartners += joinPartners;

        } catch (Exception e) {
            System.err.println(
                    "error while processing reportInput() in StreamMonitor. Error: "
                            + e.getMessage());
        }
        //        if (size1 != 0 && size2 != 0) {
        //            this.joinSelectivities.add((double) joinPartners / (double) (size1 * size2));
        //        }
    }

    public <T> void reportWindowLength(T state) {
        try {
            if (this.disableStreamMonitor) {
                return;
            }
            int length;
            if (state instanceof Long || state instanceof Double || state instanceof Integer) {
                length = 1;
            } else {
                try {
                    length = ((ArrayList<Tuple>) state).size();
                } catch (ClassCastException e1) {
                    throw new IllegalStateException(e1);
                }
            }
            this.windowLengths.add(length);
        } catch (Exception e) {
            System.err.println(
                    "error while processing reportInput() in StreamMonitor. Error: "
                            + e.getMessage());
        }
    }

    private void checkIfObservationEnd() {
        if (this.disableStreamMonitor) {
            return;
        }
        if (!observationMade) {
            long elapsedTime = System.nanoTime() - this.startTime;
            if (elapsedTime > duration) {
                observationMade = true;
                // put tupleWidthIn into description
                if (this.operator instanceof WrappingFunction) { // join operator
                    description.put(
                            "tupleWidthIn", joinInputWidthLeftSide + joinInputWidthRightSide);

                } else if (!(this.operator instanceof SourceFunction)) {
                    // it's not a join operator, so the tupleWidthIn can be put into description
                    description.put("tupleWidthIn", tupleWidthIn);
                }
                description.put(
                        "outputRate", ((double) this.outputCounter * 1e9 / (double) elapsedTime));
                if (!(this.operator instanceof SourceFunction)) {
                    description.put(
                            "inputRate", ((double) this.inputCounter * 1e9 / (double) elapsedTime));
                }
                if (this.operator instanceof WrappingFunction) {
                    double joinSelectivity =
                            (double) this.joinPartners / (double) (this.joinSize1 * this.joinSize2);
                    if (Double.isNaN(joinSelectivity)) {
                        joinSelectivity = 0;
                    }
                    description.put("realSelectivity", joinSelectivity);
                }
                if (this.operator instanceof StreamFilter) {
                    description.put(
                            "realSelectivity",
                            ((double) this.outputCounter / (double) this.inputCounter));
                } else if (this.operator instanceof WindowOperator) {
                    double averageWindowLength =
                            this.windowLengths.stream()
                                    .mapToDouble(val -> val)
                                    .average()
                                    .orElse(0.0);
                    if (averageWindowLength == 0) {
                        description.put("realSelectivity", 0.0);
                    } else {
                        description.put("realSelectivity", ((double) 1 / averageWindowLength));
                    }
                }
                addHostAndTaskMetrics(description);
                addHardwareMetrics(description);
                JSONObject json = new JSONObject();
                json.putAll(description);

                // Log data characteristics either locally or in database
                if (this.description.get("id") == null) {
                    System.out.println(
                            "StreamMonitor: cannot find id to log to for " + this.operator);
                }
                // Map<String, String> allVariables = this.operator.metrics.getAllVariables();
                if (this.description.get("id") != null) {
                    StreamMonitorMongoClient.singleton(this.config)
                            .getMongoCollectionObservations()
                            .insertOne(json);
                }
            }
        }
    }

    private HashMap<String, Object> addHostAndTaskMetrics(HashMap<String, Object> description) {
        OperatorMetricGroup metrics;
        // get metrics from operator. In case of a join, get the metrics instead from
        // the window operator
        if (this.operator instanceof WrappingFunction && this.windowOperator != null) {
            metrics = this.windowOperator.metrics;
        } else if (this.operator instanceof RichSourceFunction) {
            metrics = ((RichSourceFunction) this.operator).getRuntimeContext().getMetricGroup();
        } else if (this.operator instanceof RichParallelSourceFunction) {
            metrics =
                    ((RichParallelSourceFunction) this.operator)
                            .getRuntimeContext()
                            .getMetricGroup();
        } else {
            metrics = ((AbstractStreamOperator) this.operator).metrics;
        }
        if (metrics != null) {
            Map<String, String> allVariables = metrics.getAllVariables();
            String host = "no-host-determined";
            try {
                host =
                        new String(Files.readAllBytes(Paths.get("/etc/machine-id")))
                                .replaceAll("[^0-9,a-z,A-Z]", "");
            } catch (IOException e) {
                System.err.println("Cannot get hostname to log out in observations.");
                e.printStackTrace();
            }
            String component = allVariables.get("<task_id>");
            this.description.put("component", component);
            if (host != null) {
                this.description.put("host", host);
            }
        }
        return description;
    }

    private HashMap<String, Object> addHardwareMetrics(HashMap<String, Object> description) {
        Integer networkSpeed = 0;
        // get network speed
        try {
            // first determine default network interface
            Process p =
                    new ProcessBuilder(
                                    "/bin/sh", "-c", "route | grep '^default' | grep -o '[^ ]*$'")
                            .start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            StringBuilder builder = new StringBuilder();
            String line = null;
            while (true) {
                if (!((line = reader.readLine()) != null)) {
                    break;
                }
                builder.append(line);
                builder.append(System.getProperty("line.separator"));
            }
            String defaultNetworkInterface = "";
            defaultNetworkInterface = builder.toString();
            if (defaultNetworkInterface.equals("")) {
                defaultNetworkInterface = "eth0";
            }
            // determine network speed of default network interface
            p =
                    new ProcessBuilder(
                                    "/bin/sh",
                                    "-c",
                                    "cat /sys/class/net/" + defaultNetworkInterface + "/speed")
                            .start();
            reader = new BufferedReader(new InputStreamReader(p.getInputStream()));
            builder = new StringBuilder();
            while (true) {
                if (!((line = reader.readLine()) != null)) {
                    break;
                }
                builder.append(line);
            }
            networkSpeed = Integer.valueOf(builder.toString().replaceAll("[^0-9]", ""));
        } catch (Exception e) {
            e.printStackTrace();
        }

        SystemInfo si = new SystemInfo();
        HardwareAbstractionLayer hal = si.getHardware();
        description.put("networkLinkSpeed", networkSpeed);
        description.put("physicalCPUCores", hal.getProcessor().getPhysicalProcessorCount());
        description.put("totalMemory", hal.getMemory().getTotal());
        description.put("maxCPUFreq", hal.getProcessor().getMaxFreq());
        description.put(
                "avgCPULoad", hal.getProcessor().getSystemCpuLoadBetweenTicks(prevTicks) * 100);
        return description;
    }

    public void reportJoinWindowOperator(WindowOperator windowOperator) {
        this.windowOperator = windowOperator;
    }

    private <T> int getTupleSize(T input) {
        try {
            if (input instanceof Tuple) {
                Tuple dt = (Tuple) input;
                return dt.getArity();
            } else if (input instanceof ArrayList) {
                ArrayList<StreamRecord<Tuple>> streamRecords =
                        (ArrayList<StreamRecord<Tuple>>) input;
                return streamRecords.get(0).getValue().getArity();
            } else if (input instanceof CoGroupedStreams.TaggedUnion) {
                CoGroupedStreams.TaggedUnion<Tuple, Tuple> combinedTuple =
                        (CoGroupedStreams.TaggedUnion<Tuple, Tuple>) input;
                if (combinedTuple.isOne()) {
                    return combinedTuple.getOne().getArity();
                } else {
                    return combinedTuple.getTwo().getArity();
                }
            }
            System.err.println("Cannot get tuple size because class not known to StreamMonitor");
            return -1;
        } catch (Exception e) {
            System.err.println("Cannot get tuple size: " + e);
            return -1;
        }
    }
}
