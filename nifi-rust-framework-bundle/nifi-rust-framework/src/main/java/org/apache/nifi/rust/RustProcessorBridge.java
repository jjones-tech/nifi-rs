/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.rust;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import org.apache.nifi.rust.grpc.*;
import org.apache.nifi.rust.grpc.RustProcessorServiceGrpc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * gRPC client bridge to the Rust processor runtime.
 *
 * This class manages the connection to the Rust gRPC server and provides
 * methods to invoke processor lifecycle methods.
 */
public class RustProcessorBridge implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(RustProcessorBridge.class);

    private final ManagedChannel channel;
    private final RustProcessorServiceGrpc.RustProcessorServiceBlockingStub blockingStub;

    /**
     * Create a new bridge to the Rust runtime.
     *
     * @param host Hostname of the Rust runtime
     * @param port Port of the Rust runtime
     */
    public RustProcessorBridge(String host, int port) {
        this.channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        this.blockingStub = RustProcessorServiceGrpc.newBlockingStub(channel);
        logger.info("Created gRPC channel to Rust runtime at {}:{}", host, port);
    }

    /**
     * Get all available processor types from the Rust runtime.
     *
     * @return List of processor type information
     */
    public List<ProcessorTypeInfo> getProcessorTypes() {
        try {
            ProcessorTypesResponse response = blockingStub.getProcessorTypes(Empty.getDefaultInstance());
            return response.getProcessorTypesList();
        } catch (StatusRuntimeException e) {
            logger.error("Failed to get processor types", e);
            throw new RuntimeException("Failed to get processor types from Rust runtime", e);
        }
    }

    /**
     * Create a new processor instance in the Rust runtime.
     *
     * @param processorType The type name of the processor
     * @param processorId Unique ID for this instance
     * @param processorName User-defined name
     * @return Handle to the created processor
     */
    public ProcessorHandle createProcessor(String processorType, String processorId, String processorName) {
        try {
            CreateProcessorRequest request = CreateProcessorRequest.newBuilder()
                    .setProcessorType(processorType)
                    .setProcessorId(processorId)
                    .setProcessorName(processorName)
                    .build();
            return blockingStub.createProcessor(request);
        } catch (StatusRuntimeException e) {
            logger.error("Failed to create processor: {}", processorType, e);
            throw new RuntimeException("Failed to create processor: " + processorType, e);
        }
    }

    /**
     * Destroy a processor instance.
     *
     * @param handle Handle to the processor to destroy
     */
    public void destroyProcessor(ProcessorHandle handle) {
        try {
            blockingStub.destroyProcessor(handle);
        } catch (StatusRuntimeException e) {
            logger.error("Failed to destroy processor: {}", handle.getProcessorId(), e);
        }
    }

    /**
     * Call onScheduled on a processor.
     *
     * @param processorId The processor instance ID
     * @param properties Configured properties
     * @param maxConcurrentTasks Maximum concurrent tasks
     * @param schedulingPeriodMillis Scheduling period
     * @return Response indicating success/failure
     */
    public ScheduledResponse onScheduled(
            String processorId,
            Map<String, PropertyValue> properties,
            int maxConcurrentTasks,
            long schedulingPeriodMillis) {
        try {
            ScheduledRequest.Builder builder = ScheduledRequest.newBuilder()
                    .setProcessorId(processorId)
                    .setMaxConcurrentTasks(maxConcurrentTasks)
                    .setSchedulingPeriodMillis(schedulingPeriodMillis);

            for (Map.Entry<String, PropertyValue> entry : properties.entrySet()) {
                builder.addProperties(entry.getValue());
            }

            return blockingStub.onScheduled(builder.build());
        } catch (StatusRuntimeException e) {
            logger.error("Failed to schedule processor: {}", processorId, e);
            throw new RuntimeException("Failed to schedule processor: " + processorId, e);
        }
    }

    /**
     * Call onStopped on a processor.
     *
     * @param processorId The processor instance ID
     * @return Response indicating success/failure
     */
    public StoppedResponse onStopped(String processorId) {
        try {
            StoppedRequest request = StoppedRequest.newBuilder()
                    .setProcessorId(processorId)
                    .build();
            return blockingStub.onStopped(request);
        } catch (StatusRuntimeException e) {
            logger.error("Failed to stop processor: {}", processorId, e);
            throw new RuntimeException("Failed to stop processor: " + processorId, e);
        }
    }

    /**
     * Call onTrigger on a processor.
     *
     * @param processorId The processor instance ID
     * @param inputFlowFiles List of FlowFiles to process
     * @return Response with transferred FlowFiles
     */
    public TriggerResponse onTrigger(String processorId, List<FlowFileProto> inputFlowFiles) {
        try {
            TriggerRequest request = TriggerRequest.newBuilder()
                    .setProcessorId(processorId)
                    .addAllInputFlowfiles(inputFlowFiles)
                    .build();
            return blockingStub.onTrigger(request);
        } catch (StatusRuntimeException e) {
            logger.error("Failed to trigger processor: {}", processorId, e);
            throw new RuntimeException("Failed to trigger processor: " + processorId, e);
        }
    }

    /**
     * Check if the Rust runtime is healthy.
     *
     * @return true if healthy
     */
    public boolean isHealthy() {
        try {
            HealthResponse response = blockingStub.healthCheck(Empty.getDefaultInstance());
            return response.getHealthy();
        } catch (StatusRuntimeException e) {
            return false;
        }
    }

    /**
     * Get the Rust runtime version.
     *
     * @return Version string or null if unavailable
     */
    public String getRuntimeVersion() {
        try {
            HealthResponse response = blockingStub.healthCheck(Empty.getDefaultInstance());
            return response.getVersion();
        } catch (StatusRuntimeException e) {
            return null;
        }
    }

    @Override
    public void close() {
        try {
            channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            channel.shutdownNow();
        }
    }
}
