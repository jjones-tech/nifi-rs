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
package org.apache.nifi.rust.processors;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.rust.RustProcessor;

/**
 * ConsumeFrostBus processor - consumes jobs from frost-bus.
 *
 * This Java class is a facade that delegates to the Rust implementation.
 */
@Tags({"frost", "bus", "queue", "consume", "rust"})
@CapabilityDescription("Consumes jobs from frost-bus (Valkey/Redis) and creates FlowFiles. " +
        "This processor is implemented in Rust for optimal performance.")
@InputRequirement(InputRequirement.Requirement.INPUT_FORBIDDEN)
@WritesAttributes({
        @WritesAttribute(attribute = "frost.job.id", description = "The unique ID of the frost-bus job"),
        @WritesAttribute(attribute = "frost.job.type", description = "The type of the job"),
        @WritesAttribute(attribute = "frost.workspace.id", description = "The workspace ID (if multi-tenant)"),
        @WritesAttribute(attribute = "frost.project.id", description = "The project ID (if multi-tenant)")
})
public class ConsumeFrostBus extends RustProcessor {

    @Override
    protected String getRustProcessorType() {
        return "org.apache.nifi.frost.ConsumeFrostBus";
    }
}
