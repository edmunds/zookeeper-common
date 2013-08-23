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
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A {@code ZooKeeperConnection} represents a connection to a ZooKeeper server cluster.
 * <p/>
 * A ZooKeeperConnection is a wrapper around a ZooKeeper client object. It provides automatic recovery from expired
 * sessions, advanced configuration via a {@link ZooKeeperConfig} object and a number of convenience methods (e.g. to
 * create nodes).
 * <p/>
 * To use this class, first create or inject a ZooKeeperConnection object. Then add listeners with the {@link
 * #addListener(ZooKeeperConnectionListener)} method and add any initializers using the {@link
 * #addInitializer(ZooKeeperInitializer)} method. Then call the {@link #connect()} method to connect to with the
 * ZooKeeper server.
 * <p/>
 * Other objects may also add themselves as connection listeners and contribute their own initializers rather than
 * performing that work in a central location (which is usually the same object that calls the {@link #connect()}
 * method). However, if you use this approach, be sure that all listeners and initializers are added before the {@link
 * #connect()} method is called.
 *
 * @author Ryan Holmes
 */
@Component
public class ZooKeeperConnection {

    private static final Logger logger = Logger.getLogger(ZooKeeperConnection.class);

    private static final List<ACL> DEFAULT_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    private static final long CONNECT_RETRY_WAIT_MILLIS = 5000;

    private final String hostName;
    private final int port;
    private final String pathPrefix;
    private final int sessionTimeout;
    private final int dnsRetryCount;
    private final Set<ZooKeeperConnectionListener> listeners = new HashSet<ZooKeeperConnectionListener>();
    private final Set<ZooKeeperInitializer> initializers = new HashSet<ZooKeeperInitializer>();
    private final LinkedList<ZooKeeperInitializer> initializerQueue = new LinkedList<ZooKeeperInitializer>();
    private ZooKeeper zk;
    private ZooKeeperConnectionState state;

    private final DefaultWatcher defaultWatcher = new DefaultWatcher();

    /**
     * Constructs a new ZooKeeperConnection with the given configuration.
     *
     * @param config the ZooKeeper configuration bean
     */
    @Autowired
    public ZooKeeperConnection(ZooKeeperConfig config) {
        this(config.getHostName(), config.getPort(), config.getPathPrefix(),
                config.getSessionTimeout(), config.getDnsRetryCount());

        config.validate();
    }

    /**
     * Constructs a new ZooKeeperConnection with the given parameters.
     *
     * @param hostName       server host name
     * @param port           server port
     * @param pathPrefix     connection path prefix
     * @param sessionTimeout session timeout in milliseconds
     * @param dnsRetryCount  the number of times dns lookup should be attempted.
     */
    public ZooKeeperConnection(String hostName, int port, String pathPrefix, int sessionTimeout, int dnsRetryCount) {
        this.hostName = hostName;
        this.port = port;
        this.pathPrefix = pathPrefix;
        this.sessionTimeout = sessionTimeout;
        this.state = ZooKeeperConnectionState.CLOSED;
        this.dnsRetryCount = dnsRetryCount;
    }

    /**
     * Adds a connection listener.
     *
     * @param listener the listener to add
     * @return true if the listener was added, false otherwise
     */
    public boolean addListener(ZooKeeperConnectionListener listener) {
        boolean added = false;
        if (listener != null) {
            synchronized (listeners) {
                added = listeners.add(listener);
            }
        }
        return added;
    }

    /**
     * Removes a connection listener.
     *
     * @param listener the listener to remove
     * @return true if the listener was added, false otherwise
     */
    public boolean removeListener(ZooKeeperConnectionListener listener) {
        boolean removed = false;
        if (listener != null) {
            synchronized (listeners) {
                removed = listeners.remove(listener);
            }
        }
        return removed;
    }

    /**
     * Adds an initializer.
     *
     * @param initializer the initializer to add
     * @return true if the initializer was added, false otherwise
     */
    public boolean addInitializer(ZooKeeperInitializer initializer) {
        boolean added = false;
        if (initializer != null) {
            initializer.setConnection(this);
            added = initializers.add(initializer);
        }
        return added;
    }

    /**
     * Removes an initializer.
     *
     * @param initializer the initializer to remove
     * @return true if the initializer was removed, false otherwise
     */
    public boolean removeInitializer(ZooKeeperInitializer initializer) {
        boolean removed = false;
        if (initializer != null) {
            initializer.setConnection(null);
            removed = initializers.remove(initializer);
        }
        return removed;
    }

    /**
     * Connects to the ZooKeeper server.
     * <p/>
     * This method initializes communication with the ZooKeeper server. It should typically be called after all
     * listeners have been registered with the {@link #addListener(ZooKeeperConnectionListener)} method to ensure that
     * the listeners are notified of the initial connection.
     */
    public void connect() {
        if (StringUtils.isBlank(hostName)) {
            logger.warn("ZooKeeper hostname is blank - will not try to connect");
            return;
        }

        if (state == ZooKeeperConnectionState.CONNECTED || dnsRetryCount == 0) {
            return;
        }

        logger.debug("Connecting to ZooKeeper");

        final String connectString = getConnectString();

        if (connectString == null) {
            logger.fatal("DNS Lookup Failed for ZooKeeper: " + hostName);
            return;
        }

        boolean zkCreated = false;
        while (!zkCreated) {
            try {
                zk = new ZooKeeper(connectString, sessionTimeout, defaultWatcher);
                state = ZooKeeperConnectionState.DISCONNECTED;
                zkCreated = true;
            } catch (IOException e) {
                String message = "Cannot connect to ZooKeeper server.";
                logger.error(message, e);
                try {
                    Thread.sleep(CONNECT_RETRY_WAIT_MILLIS);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    /**
     * Closes this connection.
     */
    public void close() {
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        state = ZooKeeperConnectionState.CLOSED;

        // TODO: Notify listeners
    }

    /**
     * Reconnects to ZooKeeper.
     */
    public void reconnect() {
        if (state != ZooKeeperConnectionState.CLOSED) {
            close();
        }
        connect();
    }

    /**
     * Returns true if connected or attempting to reconnect to the ZooKeeper server.
     *
     * @return true if connected or reconnecting, false if this connection is unusable
     */

    public boolean isAlive() {
        return state != ZooKeeperConnectionState.CLOSED;
    }

    /**
     * Returns true if connected to the ZooKeeper server.
     *
     * @return true if connected, false if temporarily or permanently disconnected
     */
    public boolean isConnected() {
        return state == ZooKeeperConnectionState.CONNECTED || state == ZooKeeperConnectionState.INITIALIZED;
    }

    /**
     * Returns true if connected and initializers have been executed.
     *
     * @return true if initialized, false otherwise
     */
    public boolean isInitialized() {
        return state == ZooKeeperConnectionState.INITIALIZED;
    }

    /**
     * Returns true if this connection's session is expired.
     *
     * @return true if expired, false otherwise
     */
    public boolean isExpired() {
        return state == ZooKeeperConnectionState.EXPIRED;
    }

    /**
     * The session id for this ZooKeeperConnection. The value returned is
     * not valid until the client connects to a server and may change after a
     * re-connect.
     * <p/>
     * This method is NOT thread safe
     *
     * @return current session id
     */
    public long getSessionId() {
        return zk.getSessionId();
    }

    /**
     * Creates a persistent sequential node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @return the actual path of the created node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String createPersistent(String path, byte[] data) throws KeeperException, InterruptedException {
        return create(path, data, DEFAULT_ACL, CreateMode.PERSISTENT);
    }

    /**
     * Asynchronously creates a persistent node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @param cb   the asynchronous callback
     * @param ctx  the context object
     */
    public void createPersistent(String path, byte[] data, AsyncCallback.StringCallback cb, Object ctx) {
        create(path, data, DEFAULT_ACL, CreateMode.PERSISTENT, cb, ctx);
    }

    /**
     * Asynchronously creates a persistent node at the given path as well as any missing parent nodes.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @param cb   the asynchronous callback
     * @param ctx  the context object
     */
    public void createPersistentWithParents(String path, byte[] data, AsyncCallback.StringCallback cb, Object ctx) {

        AsyncCallback.StringCallback callback = new CreateParentsCallback(data, DEFAULT_ACL, CreateMode.PERSISTENT, cb);

        create(path, data, DEFAULT_ACL, CreateMode.PERSISTENT, callback, ctx);
    }

    /**
     * Creates a persistent sequential node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @return the actual path of the created node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String createPersistentSequential(String path, byte[] data) throws KeeperException, InterruptedException {
        return create(path, data, DEFAULT_ACL, CreateMode.PERSISTENT_SEQUENTIAL);
    }

    /**
     * Asynchronously creates a persistent sequential node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @param cb   the asynchronous callback
     * @param ctx  the context object
     */
    public void createPersistentSequential(String path, byte[] data, AsyncCallback.StringCallback cb, Object ctx) {
        create(path, data, DEFAULT_ACL, CreateMode.PERSISTENT_SEQUENTIAL, cb, ctx);
    }

    /**
     * Creates an ephemeral node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @return the actual path of the created node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String createEphemeral(String path, byte[] data) throws KeeperException, InterruptedException {
        return create(path, data, DEFAULT_ACL, CreateMode.EPHEMERAL);
    }

    /**
     * Asynchronously creates an ephemeral node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @param cb   the asynchronous callback
     * @param ctx  the context object
     */
    public void createEphemeral(String path, byte[] data, AsyncCallback.StringCallback cb, Object ctx) {
        create(path, data, DEFAULT_ACL, CreateMode.EPHEMERAL, cb, ctx);
    }

    /**
     * Creates an ephemeral sequential node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @return the actual path of the created node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     */
    public String createEphemeralSequential(String path, byte[] data) throws KeeperException, InterruptedException {
        return create(path, data, DEFAULT_ACL, CreateMode.EPHEMERAL_SEQUENTIAL);
    }

    /**
     * Asynchronously creates an ephemeral sequential node at the given path with the given data.
     *
     * @param path the node path
     * @param data the initial data for the node
     * @param cb   the asynchronous callback
     * @param ctx  the context object
     */
    public void createEphemeralSequential(String path, byte[] data, AsyncCallback.StringCallback cb, Object ctx) {
        create(path, data, DEFAULT_ACL, CreateMode.EPHEMERAL_SEQUENTIAL, cb, ctx);
    }

    /**
     * Creates a node at the given path with the given data.
     *
     * @param path       the node path
     * @param data       the initial data for the node
     * @param acl        the acl for the node
     * @param createMode specifying whether the node to be created is ephemeral and/or sequential
     * @return the actual path of the created node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooKeeper#create(String, byte[], java.util.List, org.apache.zookeeper.CreateMode)}
     */
    public String create(final String path, byte[] data, List<ACL> acl,
                         CreateMode createMode) throws KeeperException, InterruptedException {
        return zk.create(path, data, acl, createMode);
    }

    /**
     * Asynchronously creates a node at the given path with the given data.
     *
     * @param path       the node path
     * @param data       the initial data for the node
     * @param acl        the acl for the node
     * @param createMode specifying whether the node to be created is ephemeral and/or sequential
     * @param cb         the asynchronous callback
     * @param ctx        the context object
     * @see org.apache.zookeeper.ZooKeeper#create(String, byte[], java.util.List, org.apache.zookeeper.CreateMode)}
     */
    public void create(final String path, byte[] data, List<ACL> acl,
                       CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
        zk.create(path, data, acl, createMode, cb, ctx);
    }

    /**
     * Deletes the node at the given path.
     *
     * @param path    the path of the node to be deleted
     * @param version the expected node version (-1 to ignore)
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooKeeper#delete(String, int)
     */
    public void delete(final String path, int version) throws KeeperException, InterruptedException {
        zk.delete(path, version);
    }

    /**
     * Asynchronously deletes the node at the given path.
     *
     * @param path    the path of the node to be deleted
     * @param version the expected node version (-1 to ignore)
     * @param cb      the asynchronous callback
     * @param ctx     the context object
     * @see org.apache.zookeeper.ZooKeeper#delete(String, int)
     */
    public void delete(final String path, int version, AsyncCallback.VoidCallback cb, Object ctx) {
        zk.delete(path, version, cb, ctx);
    }

    /**
     * Checks whether the node at the given path exists.
     *
     * @param path    the node path
     * @param watcher explicit watcher
     * @return the stat of the node at the given path or null if no such node exists
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see ZooKeeper#exists(String, org.apache.zookeeper.Watcher)
     */
    public Stat exists(final String path, Watcher watcher) throws KeeperException, InterruptedException {
        return zk.exists(path, watcher);
    }

    /**
     * Asynchronously checks whether the node at the given path exists.
     *
     * @param path    the node path
     * @param watcher explicit watcher
     * @param cb      the asynchronous callback
     * @param ctx     the context object
     * @see ZooKeeper#exists(String, org.apache.zookeeper.Watcher)
     */
    public void exists(final String path, Watcher watcher, AsyncCallback.StatCallback cb, Object ctx) {
        zk.exists(path, watcher, cb, ctx);
    }

    /**
     * Gets the data and the stat of the node at the given path.
     *
     * @param path    the node path
     * @param watcher explicit watcher
     * @param stat    the stat of the node will be copied into this parameter
     * @return the data of the node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see ZooKeeper#getData(String, org.apache.zookeeper.Watcher, org.apache.zookeeper.data.Stat)
     */
    public byte[] getData(final String path, Watcher watcher, Stat stat) throws KeeperException, InterruptedException {
        return zk.getData(path, watcher, stat);
    }

    /**
     * Asynchronously gets the data and the stat of the node at the given path.
     *
     * @param path    the node path
     * @param watcher explicit watcher
     * @param cb      the asynchronous callback
     * @param ctx     the context object
     * @see ZooKeeper#getData(String, org.apache.zookeeper.Watcher, org.apache.zookeeper.data.Stat)
     */
    public void getData(final String path, Watcher watcher, AsyncCallback.DataCallback cb, Object ctx) {
        zk.getData(path, watcher, cb, ctx);
    }

    /**
     * Sets the data for the node at the given path.
     *
     * @param path    the node path
     * @param data    the data to set
     * @param version the expected node version (-1 to ignore)
     * @return the stat of the node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooKeeper#setData(String, byte[], int)
     */
    public Stat setData(final String path, byte[] data, int version) throws KeeperException, InterruptedException {
        return zk.setData(path, data, version);
    }

    /**
     * Asynchronously sets the data for the node at the given path.
     *
     * @param path    the node path
     * @param data    the data to set
     * @param version the expected node version (-1 to ignore)
     * @param cb      the asynchronous callback
     * @param ctx     the context object
     * @see org.apache.zookeeper.ZooKeeper#setData(String, byte[], int)
     */
    public void setData(final String path, byte[] data, int version, AsyncCallback.StatCallback cb, Object ctx) {
        zk.setData(path, data, version, cb, ctx);
    }

    /**
     * Gets the ACL and stat of the node at the given path.
     *
     * @param path the node path
     * @param stat the stat of the node will be copied to this parameter
     * @return the ACL array of the given node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooKeeper#getACL(String, org.apache.zookeeper.data.Stat)
     */
    public List<ACL> getACL(final String path, Stat stat) throws KeeperException, InterruptedException {
        return zk.getACL(path, stat);
    }

    /**
     * Asynchronously gets the ACL and stat of the node at the given path.
     *
     * @param path the node path
     * @param stat the stat of the node will be copied to this parameter
     * @param cb   the asynchronous callback
     * @param ctx  the context object
     * @see org.apache.zookeeper.ZooKeeper#getACL(String, org.apache.zookeeper.data.Stat)
     */
    public void getACL(final String path, Stat stat, AsyncCallback.ACLCallback cb, Object ctx) {
        zk.getACL(path, stat, cb, ctx);
    }

    /**
     * Sets the ACL for the node at the given path.
     *
     * @param path    the node path
     * @param acl     the ACL to set
     * @param version the expected node version
     * @return the stat of the node
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooKeeper#setACL(String, java.util.List, int)
     */
    public Stat setACL(final String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {
        return zk.setACL(path, acl, version);
    }

    /**
     * Asynchronously sets the ACL for the node at the given path.
     *
     * @param path    the node path
     * @param acl     the ACL to set
     * @param version the expected node version
     * @param cb      the asynchronous callback
     * @param ctx     the context object
     * @see org.apache.zookeeper.ZooKeeper#setACL(String, java.util.List, int)
     */
    public void setACL(final String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
        zk.setACL(path, acl, version, cb, ctx);
    }

    /**
     * Gets the children list of children of the node at the given path.
     *
     * @param path    the node path
     * @param watcher explicit watcher
     * @return an unordered array of children of the node with the given path
     * @throws KeeperException          if the server signals an error
     * @throws InterruptedException     if the transaction is interrupted
     * @throws IllegalArgumentException if an invalid path is specified
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, org.apache.zookeeper.Watcher)
     */
    public List<String> getChildren(final String path, Watcher watcher) throws KeeperException, InterruptedException {
        return zk.getChildren(path, watcher);
    }

    /**
     * Asynchronously gets the children list and stat of the node at the given path.
     *
     * @param path    the node path
     * @param watcher explicit watcher
     * @param cb      the asynchronous callback
     * @param ctx     the context object
     * @see org.apache.zookeeper.ZooKeeper#getChildren(String, org.apache.zookeeper.Watcher)
     */
    public void getChildren(final String path, Watcher watcher, AsyncCallback.Children2Callback cb, Object ctx) {
        zk.getChildren(path, watcher, cb, ctx);
    }

    /**
     * Notifies the connection that an initializer has finished.
     *
     * @param initializer the initializer sending the notification
     */
    void initializerFinished(ZooKeeperInitializer initializer) {
        initializerQueue.remove(initializer);
        startNextInitializer();
    }

    /**
     * Initializes the connection.
     * <p/>
     * Note: The connection is initialized every time we connect to the server. No distinction is made between an
     * "initial" connection and a reconnection.
     */
    private void initialize() {

        // Reset the initializer queue
        initializerQueue.clear();
        initializerQueue.addAll(initializers);

        // Start the first initializer
        startNextInitializer();
    }

    /**
     * Starts the next avaialable initializer.
     */
    private void startNextInitializer() {

        if (initializerQueue.isEmpty()) {

            // Initialization completed
            processInitialized();
            return;
        }

        // Start the next initializer
        initializerQueue.getFirst().start();
    }

    /**
     * Processes the ZooKeeper connected event.
     */
    private void processConnected() {
        // ZK client may be null on first connect event
        while (zk == null) {
            logger.warn("ZooKeeper client is null, waiting");
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("Thread interrupted", e);
            }
        }

        if (state != ZooKeeperConnectionState.CONNECTED) {
            state = ZooKeeperConnectionState.CONNECTED;
            synchronized (listeners) {
                for (ZooKeeperConnectionListener listener : listeners) {
                    listener.onConnectionStateChanged(state);
                }
            }
            initialize();
        }
    }

    private void processInitialized() {
        if (state != ZooKeeperConnectionState.INITIALIZED) {
            state = ZooKeeperConnectionState.INITIALIZED;
            synchronized (listeners) {
                for (ZooKeeperConnectionListener listener : listeners) {
                    listener.onConnectionStateChanged(state);
                }
            }
        }
    }

    /**
     * Processes the ZooKeeper disconnected event.
     */
    private void processDisconnected() {
        if (state != ZooKeeperConnectionState.DISCONNECTED) {
            state = ZooKeeperConnectionState.DISCONNECTED;
            synchronized (listeners) {
                for (ZooKeeperConnectionListener listener : listeners) {
                    listener.onConnectionStateChanged(state);
                }
            }
        }
    }

    /**
     * Processes the ZooKeeper session expired event.
     */
    private void processSessionExpired() {
        zk = null;
        if (state != ZooKeeperConnectionState.EXPIRED) {
            state = ZooKeeperConnectionState.EXPIRED;
            synchronized (listeners) {
                for (ZooKeeperConnectionListener listener : listeners) {
                    listener.onConnectionStateChanged(state);
                }
            }
            reconnect();
        }
    }

    private String getConnectString() {
        for (int i = 0; i < dnsRetryCount; i++) {
            try {
                return buildConnectString();
            } catch (UnknownHostException e) {
                logger.error("Cannot build ZooKeeper connect string", e);
                try {
                    Thread.sleep(CONNECT_RETRY_WAIT_MILLIS);
                } catch (InterruptedException e1) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        return null;
    }

    /**
     * Builds a ZooKeeper connection string based on the current configuration data.
     *
     * @return a ZooKeeper connection string
     * @throws java.net.UnknownHostException if the {@link #hostName} cannot be found in DNS
     */
    private String buildConnectString() throws UnknownHostException {
        Validate.notEmpty(hostName, "Host name must not be empty.");
        Validate.isTrue(port > 0, "Invalid port: " + port);

        final InetAddress[] inetAddresses = InetAddress.getAllByName(hostName);
        final StringBuilder connectString = new StringBuilder();

        for (InetAddress inetAddress : inetAddresses) {
            if (inetAddress instanceof Inet4Address) {
                if (connectString.length() > 0) {
                    connectString.append(",");
                }
                connectString.append(inetAddress.getHostAddress());
                connectString.append(":");
                connectString.append(port);
            }
        }
        connectString.append(pathPrefix);
        return connectString.toString();
    }

    /**
     * A watcher that intercepts ZooKeeper connection state changes.
     */
    private class DefaultWatcher implements Watcher {

        @Override
        public void process(WatchedEvent event) {
            if (logger.isDebugEnabled()) {
                logger.debug(String.format("Processing WatchEvent: %s", event));
            }

            if (event.getType() == Event.EventType.None) {
                // The state of the connection has changed.
                switch (event.getState()) {
                    case Disconnected:
                        processDisconnected();
                        break;
                    case SyncConnected:
                        processConnected();
                        break;
                    case Expired:
                        processSessionExpired();
                        break;
                    default:
                        // Ignore other states
                }
            }
        }
    }

    /**
     * Callback to recursively create parent nodes.
     * <p/>
     * The algorithm is optimistic in that it first attempts to create the leaf node and then works up the path
     * attempting to create parent nodes, keeping track of missing paths along the way. As soon as it successfully
     * creates a node, it creates nodes at the missing paths in order. The efficiency of this approach increases with
     * the depth of the parent hierarchy as it avoids repeatedly checking existing parent nodes for existence.
     */
    private class CreateParentsCallback implements AsyncCallback.StringCallback {

        private final byte[] data;
        private final List<ACL> acl;
        private final CreateMode createMode;
        private final StringCallback origCallback;
        private final LinkedList<String> missingPaths = new LinkedList<String>();

        CreateParentsCallback(byte[] data,
                              List<ACL> acl,
                              CreateMode createMode,
                              StringCallback origCallback) {
            this.data = data;
            this.acl = acl;
            this.createMode = createMode;
            this.origCallback = origCallback;
        }

        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            Code result = Code.get(rc);

            if (result == Code.NONODE) {

                // Parent node doesn't exist, add current path to list of missing paths
                missingPaths.addFirst(path);

                // Create the parent node
                String parentDir;
                int separatorIndex = path.lastIndexOf('/');
                if (separatorIndex > 0) {
                    parentDir = path.substring(0, separatorIndex);
                } else if (separatorIndex == 0) {
                    parentDir = "/";
                } else {
                    // Separator not found
                    String message = "Root node does not exist.";
                    logger.error(message);
                    throw new RuntimeException(message);
                }
                create(parentDir, new byte[0], acl, CreateMode.PERSISTENT, this, ctx);
            } else if (result == Code.OK || result == Code.NODEEXISTS) {

                if (missingPaths.isEmpty()) {

                    // No more paths to create, pass through to original callback
                    if (origCallback != null) {
                        origCallback.processResult(rc, path, ctx, name);
                    }
                } else {

                    // Create the next missing path
                    String nextPath = missingPaths.getFirst();
                    if (missingPaths.size() == 1) {

                        // Next path is the leaf node.
                        create(nextPath, data, acl, createMode, origCallback, ctx);
                    } else {

                        // Next path is a parent node
                        create(nextPath, data, acl, CreateMode.PERSISTENT, this, ctx);
                    }
                    missingPaths.removeFirst();
                }
            } else if (origCallback != null) {

                // Error occurred, pass to original callback
                origCallback.processResult(rc, path, ctx, name);
            }
        }
    }
}
