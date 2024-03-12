/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.HashMap;

/** A {@link StreamOperator} for executing {@link FilterFunction FilterFunctions}. */
@Internal
public class StreamFilter<IN> extends AbstractUdfStreamOperator<IN, FilterFunction<IN>>
        implements OneInputStreamOperator<IN, IN> {

    private static final long serialVersionUID = 1L;
    // Flink-Observation: added streamMonitor and executionConfig
    private StreamMonitor streamMonitor;
    ExecutionConfig executionConfig;

    // Flink-Observation: init StreamFilter without description to preserve compatibility
    public StreamFilter(FilterFunction<IN> filterFunction) {
        super(filterFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
        // Flink-Observation: init streamMonitor
        streamMonitor = new StreamMonitor(null, this);
    }

    // Flink-Observation: init StreamFilter with description
    public StreamFilter(FilterFunction<IN> filterFunction, HashMap<String, Object> description) {
        super(filterFunction);
        chainingStrategy = ChainingStrategy.ALWAYS;
        // Flink-Observation: init streamMonitor
        streamMonitor = new StreamMonitor(description, this);
    }

    @Override
    public void processElement(StreamRecord<IN> element) throws Exception {
        // Flink-Observation: set executionConfig
        if (this.executionConfig == null) {
            this.executionConfig = getExecutionConfig();
        }
        // Flink-Observation: report input and execution config
        streamMonitor.reportInput(element.getValue(), this.executionConfig);
        if (userFunction.filter(element.getValue())) {
            output.collect(element);
            // Flink-Observation: report output to StreamMonitor
            streamMonitor.reportOutput(element.getValue());
        }
    }
}
