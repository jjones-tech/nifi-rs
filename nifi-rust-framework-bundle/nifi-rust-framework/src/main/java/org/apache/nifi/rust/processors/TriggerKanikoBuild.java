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
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.rust.RustProcessor;

/**
 * TriggerKanikoBuild processor - triggers kaniko builds in Kubernetes.
 *
 * This Java class is a facade that delegates to the Rust implementation.
 */
@Tags({"frost", "kaniko", "build", "kubernetes", "docker", "rust"})
@CapabilityDescription("Triggers kaniko container image builds in Kubernetes. " +
        "The FlowFile content should contain build configuration JSON. " +
        "This processor is implemented in Rust for optimal performance.")
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@ReadsAttributes({
        @ReadsAttribute(attribute = "frost.workspace.id", description = "The workspace ID for workspace-scoped builds")
})
@WritesAttributes({
        @WritesAttribute(attribute = "frost.build.job", description = "The Kubernetes job name for the build"),
        @WritesAttribute(attribute = "frost.build.image", description = "The built image tag")
})
public class TriggerKanikoBuild extends RustProcessor {

    @Override
    protected String getRustProcessorType() {
        return "org.apache.nifi.frost.TriggerKanikoBuild";
    }
}
