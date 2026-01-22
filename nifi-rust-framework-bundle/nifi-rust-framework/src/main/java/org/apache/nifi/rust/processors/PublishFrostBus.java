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
import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.rust.RustProcessor;

/**
 * PublishFrostBus processor - publishes jobs to frost-bus.
 *
 * This Java class is a facade that delegates to the Rust implementation.
 */
@Tags({"frost", "bus", "queue", "publish", "rust"})
@CapabilityDescription("Publishes FlowFiles as jobs to frost-bus (Valkey/Redis). " +
        "The FlowFile content becomes the job payload. " +
        "This processor is implemented in Rust for optimal performance.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({
        @ReadsAttribute(attribute = "frost.job.type", description = "The job type to publish as"),
        @ReadsAttribute(attribute = "frost.workspace.id", description = "The workspace ID (for multi-tenancy)"),
        @ReadsAttribute(attribute = "frost.project.id", description = "The project ID (for multi-tenancy)")
})
public class PublishFrostBus extends RustProcessor {

    @Override
    protected String getRustProcessorType() {
        return "org.apache.nifi.frost.PublishFrostBus";
    }
}
