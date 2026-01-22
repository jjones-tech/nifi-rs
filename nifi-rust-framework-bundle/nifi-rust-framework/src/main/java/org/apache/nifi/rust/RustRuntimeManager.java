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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Manages the lifecycle of the Rust runtime process.
 *
 * This class is responsible for starting, stopping, and monitoring
 * the Rust gRPC server that hosts Rust processors.
 */
public class RustRuntimeManager {
    private static final Logger logger = LoggerFactory.getLogger(RustRuntimeManager.class);

    private static final String DEFAULT_BINARY_NAME = "nifi-rust-runtime";
    private static final int DEFAULT_PORT = 50051;
    private static final int STARTUP_TIMEOUT_SECONDS = 30;

    private static RustRuntimeManager instance;

    private Process runtimeProcess;
    private final String binaryPath;
    private final int port;
    private final String host;

    private RustRuntimeManager(String binaryPath, String host, int port) {
        this.binaryPath = binaryPath;
        this.host = host;
        this.port = port;
    }

    /**
     * Get or create the singleton instance.
     *
     * @return The runtime manager instance
     */
    public static synchronized RustRuntimeManager getInstance() {
        if (instance == null) {
            String binaryPath = System.getProperty("nifi.rust.runtime.binary",
                    System.getenv().getOrDefault("NIFI_RUST_RUNTIME_BINARY", DEFAULT_BINARY_NAME));
            String host = System.getProperty("nifi.rust.runtime.host",
                    System.getenv().getOrDefault("NIFI_RUST_RUNTIME_HOST", "localhost"));
            int port = Integer.parseInt(System.getProperty("nifi.rust.runtime.port",
                    System.getenv().getOrDefault("NIFI_RUST_RUNTIME_PORT", String.valueOf(DEFAULT_PORT))));

            instance = new RustRuntimeManager(binaryPath, host, port);
        }
        return instance;
    }

    /**
     * Start the Rust runtime process.
     *
     * @throws IOException If the process cannot be started
     */
    public synchronized void startRuntime() throws IOException {
        if (isRuntimeRunning()) {
            logger.info("Rust runtime is already running");
            return;
        }

        File binaryFile = new File(binaryPath);
        if (!binaryFile.exists()) {
            // Try to find in PATH or standard locations
            binaryFile = findBinary();
        }

        if (binaryFile == null || !binaryFile.exists()) {
            throw new IOException("Rust runtime binary not found: " + binaryPath);
        }

        logger.info("Starting Rust runtime from: {}", binaryFile.getAbsolutePath());

        ProcessBuilder pb = new ProcessBuilder(
                binaryFile.getAbsolutePath()
        );

        pb.environment().put("NIFI_RUST_PORT", String.valueOf(port));
        pb.environment().put("RUST_LOG", "info");

        pb.redirectErrorStream(true);
        pb.inheritIO();

        runtimeProcess = pb.start();

        // Wait for startup
        if (!waitForRuntime(STARTUP_TIMEOUT_SECONDS)) {
            stopRuntime();
            throw new IOException("Rust runtime failed to start within " + STARTUP_TIMEOUT_SECONDS + " seconds");
        }

        logger.info("Rust runtime started successfully on port {}", port);
    }

    /**
     * Stop the Rust runtime process.
     */
    public synchronized void stopRuntime() {
        if (runtimeProcess != null) {
            logger.info("Stopping Rust runtime");

            runtimeProcess.destroy();

            try {
                if (!runtimeProcess.waitFor(10, TimeUnit.SECONDS)) {
                    runtimeProcess.destroyForcibly();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                runtimeProcess.destroyForcibly();
            }

            runtimeProcess = null;
            logger.info("Rust runtime stopped");
        }
    }

    /**
     * Check if the runtime is running.
     *
     * @return true if running
     */
    public boolean isRuntimeRunning() {
        if (runtimeProcess == null) {
            return false;
        }

        try {
            runtimeProcess.exitValue();
            return false; // Process has exited
        } catch (IllegalThreadStateException e) {
            return true; // Process is still running
        }
    }

    /**
     * Get a bridge to the runtime.
     *
     * @return A new bridge instance
     */
    public RustProcessorBridge getBridge() {
        return new RustProcessorBridge(host, port);
    }

    /**
     * Get the runtime host.
     *
     * @return Host name
     */
    public String getHost() {
        return host;
    }

    /**
     * Get the runtime port.
     *
     * @return Port number
     */
    public int getPort() {
        return port;
    }

    private boolean waitForRuntime(int timeoutSeconds) {
        long deadline = System.currentTimeMillis() + (timeoutSeconds * 1000L);

        while (System.currentTimeMillis() < deadline) {
            try (RustProcessorBridge bridge = new RustProcessorBridge(host, port)) {
                if (bridge.isHealthy()) {
                    return true;
                }
            } catch (Exception e) {
                // Not ready yet
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }

        return false;
    }

    private File findBinary() {
        // Check standard locations
        String[] searchPaths = {
                ".",
                "./target/release",
                "./target/debug",
                "/usr/local/bin",
                "/usr/bin",
                System.getProperty("user.home") + "/.cargo/bin"
        };

        for (String path : searchPaths) {
            File f = new File(path, DEFAULT_BINARY_NAME);
            if (f.exists() && f.canExecute()) {
                return f;
            }

            // Try with .exe on Windows
            f = new File(path, DEFAULT_BINARY_NAME + ".exe");
            if (f.exists() && f.canExecute()) {
                return f;
            }
        }

        return null;
    }
}
