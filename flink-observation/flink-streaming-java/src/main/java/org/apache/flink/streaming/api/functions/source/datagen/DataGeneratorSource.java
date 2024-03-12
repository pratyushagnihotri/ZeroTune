/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.functions.source.datagen;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.operators.StreamMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;

/**
 * A data generator source that abstract data generator. It can be used to easy startup/test for
 * streaming job and performance testing. It is stateful, re-scalable, possibly in parallel.
 */
@Experimental
public class DataGeneratorSource<T> extends RichParallelSourceFunction<T> {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(DataGeneratorSource.class);

    private final DataGenerator<T> generator;
    private final long rowsPerSecond;
    @Nullable private final Long numberOfRows;
    transient volatile boolean isRunning;
    private StreamMonitor<DataGeneratorSource<T>> streamMonitor;
    private transient int outputSoFar;
    private transient int toOutput;
    private HashMap<String, Object> description;

    /**
     * Creates a source that emits records by {@link DataGenerator} without controlling emit rate.
     *
     * @param generator data generator.
     */
    public DataGeneratorSource(DataGenerator<T> generator) {
        this(generator, Long.MAX_VALUE, null, null);
    }

    public DataGeneratorSource(
            DataGenerator<T> generator, long rowsPerSecond, @Nullable Long numberOfRows) {
        this(generator, rowsPerSecond, numberOfRows, null);
    }

    /**
     * Creates a source that emits records by {@link DataGenerator}.
     *
     * @param generator data generator.
     * @param rowsPerSecond Control the emit rate.
     * @param numberOfRows Total number of rows to output.
     */
    public DataGeneratorSource(
            DataGenerator<T> generator,
            long rowsPerSecond,
            @Nullable Long numberOfRows,
            HashMap<String, Object> description) {
        this.generator = generator;
        this.rowsPerSecond = rowsPerSecond;
        this.numberOfRows = numberOfRows;
        this.description = description;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        if (numberOfRows != null) {
            final int stepSize = getRuntimeContext().getNumberOfParallelSubtasks();
            final int taskIdx = getRuntimeContext().getIndexOfThisSubtask();

            final int baseSize = (int) (numberOfRows / stepSize);
            toOutput = (numberOfRows % stepSize > taskIdx) ? baseSize + 1 : baseSize;
        }
        this.streamMonitor = new StreamMonitor<>(this.description, this);
        this.generator.open("DataGenerator", null, getRuntimeContext());
        this.isRunning = true;
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        double taskRowsPerSecond =
                (double) rowsPerSecond / getRuntimeContext().getNumberOfParallelSubtasks();
        long nextReadTime = System.currentTimeMillis();

        while (isRunning) {
            for (int i = 0; i < taskRowsPerSecond; i++) {
                if (isRunning) {
                    outputSoFar++;
                    T nextTuple = this.generator.next();
                    this.streamMonitor.reportInput(
                            nextTuple, getRuntimeContext().getExecutionConfig());
                    ctx.collect(nextTuple);
                    this.streamMonitor.reportOutput(nextTuple);
                } else {
                    return;
                }
            }

            nextReadTime += 1000;
            long toWaitMs = nextReadTime - System.currentTimeMillis();
            while (toWaitMs > 0) {
                Thread.sleep(toWaitMs);
                toWaitMs = nextReadTime - System.currentTimeMillis();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        LOG.info("generated {} rows", outputSoFar);
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
