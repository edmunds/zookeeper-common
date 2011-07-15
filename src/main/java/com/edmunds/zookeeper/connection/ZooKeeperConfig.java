/*
 * Copyright 2011 Edmunds.com, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.edmunds.zookeeper.connection;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;

/**
 * Configuration bean for a {@link ZooKeeperConnection}.
 *
 * @author Ryan Holmes
 */
public class ZooKeeperConfig {

    private static final String DEFAULT_HOST_NAME = "localhost";

    // Default port ZooKeeper uses.
    private static final int DEFAULT_PORT = 2181;

    // Default timeout before session is expired.
    private static final int DEFAULT_SESSION_TIMEOUT = 15000;

    private String hostName = DEFAULT_HOST_NAME;
    private int port = DEFAULT_PORT;
    private int sessionTimeout = DEFAULT_SESSION_TIMEOUT;
    private String pathPrefix = "";
    private int dnsRetryCount = 3;

    /**
     * Gets the host name of the ZooKeeper server cluster.
     * <p/>
     * A DNS lookup is performed on this name. This lookup may return multiple IP addresses, one for each node in the
     * cluster.
     * <p/>
     * The default is {@code localhost}.
     *
     * @return the host name of the ZooKeeper cluster.
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Sets the host name of the ZooKeeper server cluster.
     *
     * @param hostName the host name of the ZooKeeper server cluster.
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * The port the ZooKeeper servers are running on.
     * <p/>
     * The default is port {@code 2181}.
     *
     * @return the port number.
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the ZooKeeper server port number.
     *
     * @param port the port number.
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Gets the session timeout in milliseconds.
     * <p/>
     * The default is 5000 (5 seconds).
     *
     * @return the session timeout in milliseconds.
     */
    public int getSessionTimeout() {
        return sessionTimeout;
    }

    /**
     * Sets the session timeout in milliseconds.
     *
     * @param sessionTimeout the session timeout in milliseconds.
     */
    public void setSessionTimeout(int sessionTimeout) {
        this.sessionTimeout = sessionTimeout;
    }

    /**
     * Gets the path at which a ZooKeeper connection will be rooted. All other paths specified on the connection will be
     * relative to this path.
     * <p/>
     * The default is {@code null} (no path prefix).
     *
     * @return the "change root" path for a connection
     */
    public String getPathPrefix() {
        return pathPrefix;
    }

    /**
     * Sets the path at which a ZooKeeper connection will be rooted.
     *
     * @param pathPrefix the "change root" path for a connection
     */
    public void setPathPrefix(String pathPrefix) {
        this.pathPrefix = pathPrefix;
    }

    /**
     * Returns the current DNS lookup limit.
     *
     * @return the number of times DNS will be queried for the IP address of the ZooKeeper server.
     */
    public int getDnsRetryCount() {
        return dnsRetryCount;
    }

    /**
     * Sets the number of times a DNS lookup is attempted.
     *
     * @param dnsRetryCount zero is a valid value which prevents ZooKeeper connecting.
     */
    public void setDnsRetryCount(int dnsRetryCount) {
        this.dnsRetryCount = dnsRetryCount;
    }

    /**
     * Validates this ZooKeeper configuration.
     */
    public void validate() {
        Validate.notEmpty(hostName, "ZooKeeper hostName not specified");
        Validate.isTrue(port > 0, "ZooKeeper port must be greater than zero");
        Validate.isTrue(sessionTimeout > 0, "ZooKeeper session timeout must be greater than zero");
        Validate.isTrue(dnsRetryCount >= 0, "ZooKeeper DNS lookup can not be negative");

        if (!StringUtils.isBlank(pathPrefix)) {
            if (!pathPrefix.startsWith("/")) {
                throw new IllegalArgumentException("ZooKeeper path prefix must start with '/'");
            } else if (pathPrefix.endsWith("/")) {
                throw new IllegalArgumentException("ZooKeeper path prefix must not end with '/'");
            }
        }
    }
}
