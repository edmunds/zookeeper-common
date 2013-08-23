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
package com.edmunds.zookeeper.treewatcher;

import com.edmunds.zookeeper.connection.ZooKeeperConnection;
import com.edmunds.zookeeper.connection.ZooKeeperConnectionListener;
import com.edmunds.zookeeper.connection.ZooKeeperConnectionState;
import org.apache.log4j.Logger;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.List;

import static org.apache.zookeeper.AsyncCallback.Children2Callback;
import static org.apache.zookeeper.AsyncCallback.DataCallback;
import static org.apache.zookeeper.AsyncCallback.StatCallback;
import static org.apache.zookeeper.KeeperException.Code;

/**
 * Maintains the state of a tree and calls back each time the in memory tree becomes consistent with the server.
 * <p/>
 * Additionally every node in the tree has a "level" typically the root node is level 0 and the level
 * below that is 1 and so on. However it may be the case that your logic requires the root to be 1 (or n).
 * As a result it is possible to set the root level to any value, the watcher will then increments +1 for each level.
 * <p/>
 * You must pass the desired rootLevel (0, 1 or n) to the constructor.
 * Note: The watcher doesn't care what value you set.
 */
public class ZooKeeperTreeWatcher implements Watcher {

    private static final Logger logger = Logger.getLogger(ZooKeeperTreeWatcher.class);

    private final ZooKeeperConnection connection;
    private final int rootLevel;
    private final String rootPath;
    private final ZooKeeperTreeConsistentCallback callback;

    private ZooKeeperTreeState previousState;
    private ZooKeeperTreeState currentState;

    /**
     * Constructs the ZooKeeperTreeWatcher.
     * <p/>
     * If you enable autoInitialize the watcher will register a listener with the connection to initialize itself.
     * This has the advantages that:
     * <ul>
     * <li>You don't need to worry when/if you have a successful connection to ZooKeeper.</li>
     * <li>You don't need to worry about re-initializing the watcher.</li>
     * <li>You don't have to maintain a reference to the watcher yourself.</li>
     * </ul>
     * However you loose fine grained control, over your application initialization, if you use autoInitialize.
     *
     * @param connection     the connection to the ZooKeeper server, you will need to call "connect()" on the connection,
     *                       however you don't need to re-initialize this class (it will handle server dis/re-connects).
     * @param rootLevel      What level you call the root node (typically 0 or 1) - this has no effect on the watcher.
     * @param rootPath       The root path for the watcher to "watch".
     * @param callback       The callback to be used when the tree is consistent.
     * @param autoInitialize Should the watcher register a listener to automatically (re)initialize.
     */

    public ZooKeeperTreeWatcher(
            ZooKeeperConnection connection,
            int rootLevel, String rootPath,
            ZooKeeperTreeConsistentCallback callback,
            boolean autoInitialize) {

        this(connection, rootLevel, rootPath, callback);

        if (autoInitialize) {
            connection.addListener(new ConnectionInitializedListener());
        }
    }

    /**
     * Constructs the ZooKeeperTreeWatcher, without automatic initialization.
     * <p/>
     * If you use this constructor you will need to call initialize(), every time the connection is re-initialized.
     *
     * @param connection the connection to the ZooKeeper server, you will need to call "connect()" on the connection.
     * @param rootLevel  What level you call the root node (typically 0 or 1) - this has no effect on the watcher.
     * @param rootPath   The root path for the watcher to "watch".
     * @param callback   The callback to be used when the tree is consistent.
     */
    public ZooKeeperTreeWatcher(
            ZooKeeperConnection connection,
            int rootLevel, String rootPath,
            ZooKeeperTreeConsistentCallback callback) {

        this.connection = connection;
        this.rootLevel = rootLevel;
        this.rootPath = rootPath;
        this.callback = callback;
    }

    /**
     * Main entry point called after connecting to the server.
     * <p/>
     * This method is also called internal when "recoverable" errors occur.
     */
    public void initialize() {
        initializeState();

        // If the root node exists this object will entry normal operation.
        // Otherwise it sleep until the root is created.
        exists(rootPath, null);
    }

    /**
     * Called when a recoverable error occurs and the system re-initializes.
     *
     * @param path      the node that trigger the re-initialization.
     * @param eventType the event that triggered the re-initialization.
     * @param message   the error that was detected.
     */
    private void recoverableError(String path, Event.EventType eventType, String message) {
        logger.warn(eventType + " " + message + ": " + path);
        initialize();
    }

    /**
     * Called when a callback is called with a code that is not handled.
     * <p/>
     * To avoid an infinite loop condition we do not try to recover,
     * hence the state is un-defined after this method has been invoked.
     *
     * @param code the code.
     * @param path the path (may be null depending on the code).
     */
    private void unexpectedCodeError(Code code, String path) {
        logger.error("Unexpected code: " + code + " path=" + path);
        initializeState();
    }

    /**
     * Called when an unexpected watch event is detected.
     * <p/>
     * To avoid an infinite loop condition we do not try to recover,
     * hence the state is un-defined after this method has been invoked.
     *
     * @param event the unexpected event.
     */
    private void unexpectedEventError(WatchedEvent event) {
        logger.error("Unexpected event: " + event);
        initializeState();
    }

    @Override
    public void process(WatchedEvent event) {
        if (event.getState() != Watcher.Event.KeeperState.SyncConnected) {
            unexpectedEventError(event);
            return;
        }

        processEvent(event, getCurrentState());

        checkStateConsistent();
    }

    private void processEvent(WatchedEvent event, ZooKeeperTreeState state) {
        final String path = event.getPath();
        final ZooKeeperTreeNode node = state.getNode(path);
        final Event.EventType eventType = event.getType();

        switch (eventType) {
            case NodeCreated:
                if (rootPath.equals(path)) {
                    scanRootNode();
                } else {
                    recoverableError(path, eventType, "Unexpected node Created");
                }
                break;
            case NodeDataChanged:
                if (node != null) {
                    state.setFlags(path, false, node.isChildListConsistent());
                    getData(path, null);
                } else {
                    recoverableError(path, eventType, "Path is not tracked");
                }
                break;
            case NodeChildrenChanged:
                if (node != null) {
                    state.setFlags(path, node.isDataConsistent(), false);
                    getChildren(path, null);
                } else {
                    recoverableError(path, eventType, "Path is not tracked");
                }
                break;
            case NodeDeleted:
                // Apply deletions immediately.
                if (node != null) {
                    safeDelete(path);
                }
                break;
            default:
                unexpectedEventError(event);
                break;
        }
    }

    private void getChildrenResult(Code code, String path, List<String> children) {
        switch (code) {
            case OK:
                getCurrentState().setChildren(path, children);
                scanChildren(path);
                break;
            case NONODE:
                // The node we expected to exist doesn't
                if (logger.isDebugEnabled()) {
                    logger.debug("Received NO NODE code, Executing getChildren: " + path);
                }
                safeDelete(path);
                break;
            default:
                // Some error occurred so we can't rely upon the state of the tree.
                unexpectedCodeError(code, path);
                break;
        }

        checkStateConsistent();
    }

    private void getDataResult(Code code, String path, byte[] data) {
        switch (code) {
            case OK:
                getCurrentState().setData(path, data);
                break;
            case NONODE:
                // The node we expected to exist doesn't
                if (logger.isDebugEnabled()) {
                    logger.debug("Received NO NODE code, Executing getData: " + path);
                }
                safeDelete(path);
                break;
            default:
                // Some error occurred so we can't rely upon the state of the tree.
                unexpectedCodeError(code, path);
                break;
        }

        checkStateConsistent();
    }

    private void existsResult(Code code, String path, Stat stat) {
        switch (code) {
            case OK:
                if (stat == null) {
                    logger.warn("Received null stat object (ignoring): " + path);
                } else if (!rootPath.equals(path)) {
                    logger.warn("Received existence callback for unexpected path (ignoring): " + path);
                } else {
                    scanRootNode();
                }
                break;
            case NONODE:
                logger.info("Waiting for initialization to complete: " + path);
                break;
            default:
                // Some error occurred so we can't rely upon the state of the tree.
                unexpectedCodeError(code, path);
                break;
        }
    }

    private void initializeState() {
        currentState = new ZooKeeperTreeState(rootLevel, rootPath, null, false, false);
    }

    private ZooKeeperTreeState getCurrentState() {
        if (currentState == null) {
            logger.debug("getCurrentState: copying tree state");
            currentState = new ZooKeeperTreeState(previousState);
        }

        return currentState;
    }

    private void scanRootNode() {
        logger.info("Initialization Complete, watching path: " + rootPath);
        getChildren(rootPath, null);
        getData(rootPath, null);
    }

    private void scanChildren(String parentPath) {
        final ZooKeeperTreeNode parentNode = getCurrentState().getNode(parentPath);

        for (ZooKeeperTreeNode node : parentNode.getChildren().values()) {
            final String path = node.getPath();

            if (!node.isChildListConsistent()) {
                getChildren(path, null);
            }
            if (!node.isDataConsistent()) {
                getData(path, null);
            }
        }
    }

    private void checkStateConsistent() {
        if (currentState != null && currentState.getRootNode().isFullyConsistent()) {
            // Extract the root nodes from each tree.
            final ZooKeeperTreeNode previousRoot = previousState == null ? null : previousState.getRootNode();
            final ZooKeeperTreeNode currentRoot = currentState.getRootNode();

            // The current state becomes previous.
            previousState = currentState;
            currentState = null;

            // Now forward the call.
            callback.treeConsistent(previousRoot, currentRoot);
        }
    }

    private void safeDelete(String path) {
        final ZooKeeperTreeState state = getCurrentState();
        final ZooKeeperTreeNode node = state.getNode(path);

        if (node != null) {
            if (state.getRootNode() == node) {
                logger.warn("Root node was deleted, re-initializing!");
                initialize();
            } else {
                state.deleteNode(path);
            }
        }
    }

    private void getChildren(String path, Object ctx) {
        connection.getChildren(path, this,
                new Children2Callback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                        getChildrenResult(Code.get(rc), path, children);
                    }
                }, ctx);
    }

    private void getData(String path, Object ctx) {
        connection.getData(path, this,
                new DataCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                        getDataResult(Code.get(rc), path, data);
                    }
                }, ctx);
    }

    private void exists(String path, Object ctx) {
        connection.exists(path, this, new StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                existsResult(Code.get(rc), path, stat);
            }
        }, ctx);
    }

    private final class ConnectionInitializedListener implements ZooKeeperConnectionListener {
        @Override
        public void onConnectionStateChanged(ZooKeeperConnectionState state) {
            if (state == ZooKeeperConnectionState.INITIALIZED) {
                ZooKeeperTreeWatcher.this.initialize();
            }
        }
    }
}
