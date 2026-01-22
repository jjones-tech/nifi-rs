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

import com.google.protobuf.ByteString;
import org.apache.nifi.rust.grpc.*;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;

/**
 * Base class for Rust-backed NiFi processors.
 *
 * This processor acts as a facade, delegating all lifecycle and processing
 * calls to a Rust processor running in a separate process via gRPC.
 *
 * Subclasses should override getRustProcessorType() to specify which
 * Rust processor to instantiate.
 */
public abstract class RustProcessor extends AbstractProcessor {
    private static final Logger logger = LoggerFactory.getLogger(RustProcessor.class);

    // Property for configuring the Rust runtime connection
    public static final PropertyDescriptor RUST_RUNTIME_HOST = new PropertyDescriptor.Builder()
            .name("rust.runtime.host")
            .displayName("Rust Runtime Host")
            .description("Hostname of the Rust processor runtime")
            .required(true)
            .defaultValue("localhost")
            .build();

    public static final PropertyDescriptor RUST_RUNTIME_PORT = new PropertyDescriptor.Builder()
            .name("rust.runtime.port")
            .displayName("Rust Runtime Port")
            .description("Port of the Rust processor runtime")
            .required(true)
            .defaultValue("50051")
            .build();

    private RustProcessorBridge bridge;
    private String processorId;
    private ProcessorHandle processorHandle;

    // Cache for Rust processor metadata
    private List<PropertyDescriptor> rustPropertyDescriptors;
    private Set<Relationship> rustRelationships;

    /**
     * Returns the Rust processor type name.
     * Subclasses must implement this to specify which Rust processor to use.
     *
     * @return The fully qualified Rust processor type name
     */
    protected abstract String getRustProcessorType();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(RUST_RUNTIME_HOST);
        descriptors.add(RUST_RUNTIME_PORT);

        // Add Rust processor's properties if available
        if (rustPropertyDescriptors != null) {
            descriptors.addAll(rustPropertyDescriptors);
        }

        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        if (rustRelationships != null) {
            return rustRelationships;
        }
        // Default relationships before metadata is loaded
        return Collections.singleton(new Relationship.Builder()
                .name("success")
                .description("Successfully processed FlowFiles")
                .build());
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // Connect to Rust runtime
        String host = context.getProperty(RUST_RUNTIME_HOST).getValue();
        int port = context.getProperty(RUST_RUNTIME_PORT).asInteger();

        bridge = new RustProcessorBridge(host, port);

        if (!bridge.isHealthy()) {
            throw new ProcessException("Rust runtime is not healthy at " + host + ":" + port);
        }

        // Create processor instance
        processorId = getIdentifier();
        String processorName = context.getName();

        try {
            processorHandle = bridge.createProcessor(getRustProcessorType(), processorId, processorName);
            logger.info("Created Rust processor: {} ({})", processorHandle.getProcessorType(), processorId);

            // Load metadata (properties, relationships) from Rust
            loadProcessorMetadata();

            // Call onScheduled on Rust side
            Map<String, PropertyValue> properties = convertProperties(context);
            ScheduledResponse response = bridge.onScheduled(
                    processorId,
                    properties,
                    context.getMaxConcurrentTasks(),
                    0 // TODO: Get scheduling period
            );

            if (!response.getSuccess()) {
                throw new ProcessException("Rust processor onScheduled failed: " + response.getErrorMessage());
            }
        } catch (Exception e) {
            logger.error("Failed to schedule Rust processor", e);
            if (bridge != null) {
                bridge.close();
            }
            throw new ProcessException("Failed to schedule Rust processor", e);
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        // Get incoming FlowFiles
        List<FlowFile> flowFiles = session.get(100); // Batch size
        if (flowFiles.isEmpty()) {
            return;
        }

        // Convert to proto
        List<FlowFileProto> inputFlowFiles = new ArrayList<>();
        Map<String, FlowFile> flowFileMap = new HashMap<>();

        for (FlowFile flowFile : flowFiles) {
            String id = flowFile.getAttribute("uuid");
            flowFileMap.put(id, flowFile);
            inputFlowFiles.add(convertToProto(flowFile, session));
        }

        // Call Rust processor
        TriggerResponse response = bridge.onTrigger(processorId, inputFlowFiles);

        if (!response.getSuccess()) {
            // Route all to failure
            for (FlowFile flowFile : flowFiles) {
                session.transfer(flowFile, findRelationship("failure"));
            }
            logger.error("Rust processor failed: {}", response.getErrorMessage());
            return;
        }

        // Process transferred FlowFiles
        Set<String> processedIds = new HashSet<>();

        for (TransferTarget target : response.getTransferredList()) {
            FlowFileProto ffProto = target.getFlowfile();
            String relationship = target.getRelationship();

            FlowFile original = flowFileMap.get(ffProto.getId());
            if (original != null) {
                // Update existing FlowFile
                FlowFile updated = session.putAllAttributes(original, ffProto.getAttributesMap());

                // Write content if changed
                if (!ffProto.getContent().isEmpty()) {
                    updated = session.write(updated, out -> out.write(ffProto.getContent().toByteArray()));
                }

                session.transfer(updated, findRelationship(relationship));
                processedIds.add(ffProto.getId());
            } else {
                // New FlowFile created by Rust
                FlowFile newFlowFile = session.create();
                newFlowFile = session.putAllAttributes(newFlowFile, ffProto.getAttributesMap());

                if (!ffProto.getContent().isEmpty()) {
                    newFlowFile = session.write(newFlowFile, out -> out.write(ffProto.getContent().toByteArray()));
                }

                session.transfer(newFlowFile, findRelationship(relationship));
            }
        }

        // Remove FlowFiles that were consumed
        for (String removedId : response.getRemovedIdsList()) {
            FlowFile toRemove = flowFileMap.get(removedId);
            if (toRemove != null) {
                session.remove(toRemove);
                processedIds.add(removedId);
            }
        }

        // Any FlowFiles not processed should be penalized and returned to queue
        for (FlowFile flowFile : flowFiles) {
            String id = flowFile.getAttribute("uuid");
            if (!processedIds.contains(id)) {
                session.penalize(flowFile);
                session.transfer(flowFile, findRelationship("failure"));
            }
        }
    }

    @OnStopped
    public void onStopped() {
        if (bridge != null && processorId != null) {
            try {
                bridge.onStopped(processorId);
                if (processorHandle != null) {
                    bridge.destroyProcessor(processorHandle);
                }
            } finally {
                bridge.close();
                bridge = null;
            }
        }
    }

    private void loadProcessorMetadata() {
        List<ProcessorTypeInfo> types = bridge.getProcessorTypes();

        for (ProcessorTypeInfo info : types) {
            if (info.getTypeName().equals(getRustProcessorType())) {
                // Load property descriptors
                rustPropertyDescriptors = new ArrayList<>();
                for (org.apache.nifi.rust.grpc.PropertyDescriptor pd : info.getPropertiesList()) {
                    PropertyDescriptor.Builder builder = new PropertyDescriptor.Builder()
                            .name(pd.getName())
                            .displayName(pd.getDisplayName())
                            .description(pd.getDescription())
                            .required(pd.getRequired())
                            .sensitive(pd.getSensitive())
                            .expressionLanguageSupported(
                                    pd.getExpressionLanguageSupported()
                                            ? org.apache.nifi.expression.ExpressionLanguageScope.FLOWFILE_ATTRIBUTES
                                            : org.apache.nifi.expression.ExpressionLanguageScope.NONE);

                    if (!pd.getDefaultValue().isEmpty()) {
                        builder.defaultValue(pd.getDefaultValue());
                    }

                    if (!pd.getAllowableValuesList().isEmpty()) {
                        builder.allowableValues(pd.getAllowableValuesList().toArray(new String[0]));
                    }

                    rustPropertyDescriptors.add(builder.build());
                }

                // Load relationships
                rustRelationships = new HashSet<>();
                for (RelationshipInfo ri : info.getRelationshipsList()) {
                    rustRelationships.add(new Relationship.Builder()
                            .name(ri.getName())
                            .description(ri.getDescription())
                            .autoTerminateDefault(ri.getAutoTerminate())
                            .build());
                }

                return;
            }
        }

        logger.warn("Rust processor type not found: {}", getRustProcessorType());
    }

    private Map<String, PropertyValue> convertProperties(ProcessContext context) {
        Map<String, PropertyValue> properties = new HashMap<>();

        for (PropertyDescriptor descriptor : getSupportedPropertyDescriptors()) {
            if (descriptor.equals(RUST_RUNTIME_HOST) || descriptor.equals(RUST_RUNTIME_PORT)) {
                continue; // Skip framework properties
            }

            String value = context.getProperty(descriptor).getValue();
            if (value != null) {
                PropertyValue.Builder pvBuilder = PropertyValue.newBuilder()
                        .setName(descriptor.getName());

                // Try to parse as different types
                try {
                    long longValue = Long.parseLong(value);
                    pvBuilder.setIntValue(longValue);
                } catch (NumberFormatException e) {
                    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
                        pvBuilder.setBoolValue(Boolean.parseBoolean(value));
                    } else {
                        pvBuilder.setStringValue(value);
                    }
                }

                properties.put(descriptor.getName(), pvBuilder.build());
            }
        }

        return properties;
    }

    private FlowFileProto convertToProto(FlowFile flowFile, ProcessSession session) {
        FlowFileProto.Builder builder = FlowFileProto.newBuilder()
                .setId(flowFile.getAttribute("uuid"))
                .setSize(flowFile.getSize())
                .setEntryDate(flowFile.getEntryDate())
                .setLineageStartDate(flowFile.getLineageStartDate())
                .putAllAttributes(flowFile.getAttributes());

        // Read content
        if (flowFile.getSize() > 0) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            session.exportTo(flowFile, baos);
            builder.setContent(ByteString.copyFrom(baos.toByteArray()));
        }

        return builder.build();
    }

    private Relationship findRelationship(String name) {
        if (rustRelationships != null) {
            for (Relationship rel : rustRelationships) {
                if (rel.getName().equals(name)) {
                    return rel;
                }
            }
        }
        // Default fallback
        return new Relationship.Builder().name(name).build();
    }
}
