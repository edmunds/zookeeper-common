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
package com.edmunds.zookeeper.election;

import com.edmunds.zookeeper.connection.ZooKeeperConnection;
import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.AsyncCallback.DataCallback;
import org.apache.zookeeper.AsyncCallback.StringCallback;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Elects a leader among a group of coordinated processes.
 * <p/>
 * Each participant in an election begins by calling the {@link #enrollInternal()} method. A sequential
 * ephemeral node is created in the election root path to represent the participant. At this point they are considered
 * "enrolled" in the election. The participant whose node has the lowest sequence number becomes the leader and is
 * notified via the {@link ZooKeeperElectionListener#onElectionStateChange(ZooKeeperElection, boolean)}  method.
 * <p/>
 * The leader can surrender leadership (or otherwise withdraw from the election) by calling the {@link #withdraw()}
 * method. This allows another participant to become the leader. Due to the use of ephemeral nodes, leadership will also
 * be surrendered if the leader becomes disconnected from the ZooKeeper server.
 * <p/>
 * This class is based on the following algorithm:
 * <p/>
 * <pre>
 * When enrollment is started:
 * 1. Create a sequential ephemeral node 'z' under the election root path.
 * 2. Fetch the children of the root path and find the previous node, 'p'. p is the node with the largest sequence
 * number that is less than the sequence number of node z.
 * 3. Watch for changes on node p.
 *
 * When the previous node p is deleted:
 * 1. Fetch the children of the root path.
 * 2. If z has the lowest sequence number in the set of children, notify the listener that we are the leader.
 * 3. Otherwise, find a new previous node p and watch for changes.
 * </pre>
 *
 * @author David Trott
 */
public class ZooKeeperElection {
    /**
     * The logger.
     */
    private static final Logger logger = Logger.getLogger(ZooKeeperElection.class);

    private static final Comparator<String> SEQUENTIAL_COMPARATOR = new ZooKeeperSequentialValueComparator();

    /**
     * The prefix used for all election nodes (sequential ephemeral).
     */
    private static final String ELECTION_NODE_PREFIX = "node-";

    /**
     * Empty data (we don't store anything useful in the nodes).
     */
    private static final byte[] ZERO_BYTE_ARRAY = new byte[0];

    // Callbacks
    private final StringCallback callbackCreatePrimary = new StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            processResultCreatePrimary(rc, path, (ZooKeeperElectionContext) ctx, name);
        }
    };

    private final Children2Callback callbackRootChildren = new Children2Callback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            processResultRootChildren(rc, path, (ZooKeeperElectionContext) ctx, children);
        }
    };

    private final DataCallback callbackPreviousData = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            processResultPreviousData(rc, path, (ZooKeeperElectionContext) ctx);
        }
    };

    private final ZooKeeperElectionContextWatcher callbackPreviousWatcher = new ZooKeeperElectionContextWatcher() {
        @Override
        public void process(ZooKeeperElectionContext ctx, WatchedEvent event) {
            processPreviousWatcher(ctx, event);
        }
    };

    private final DataCallback callbackCurrentData = new DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            processResultCurrentData(rc, path, (ZooKeeperElectionContext) ctx);
        }
    };

    private final ZooKeeperElectionContextWatcher callbackCurrentWatcher = new ZooKeeperElectionContextWatcher() {
        @Override
        public void process(ZooKeeperElectionContext ctx, WatchedEvent event) {
            processCurrentWatcher(ctx, event);
        }
    };

    private final VoidCallback callbackDeletePriorNode = new VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            processResultDeletePriorNode(rc, path, (ZooKeeperElectionContext) ctx);
        }
    };

    /**
     * Main zookeeper connection.
     */
    private final ZooKeeperConnection connection;

    /**
     * Previous nodes that were created by this client.
     */
    private final ConcurrentHashMap<String, Boolean> electionNodes;

    /**
     * The root path for the election, sequential ephemeral nodes are created directly under this node.
     */
    private final String rootPath;

    /**
     * Set of listeners to be called back.
     */
    private final Set<ZooKeeperElectionListener> listeners;

    /**
     * The active election context for this cycle.
     */
    private volatile ZooKeeperElectionContext electionContext;

    /**
     * Are we enrolled in an election.
     */
    private volatile boolean enrolled;

    /**
     * Creates a new ZooKeeperElection with the given connection and root path.
     *
     * @param connection a ZooKeeper connection
     * @param rootPath   the root path for election member nodes
     */
    public ZooKeeperElection(ZooKeeperConnection connection, String rootPath) {
        Validate.notNull(connection);
        Validate.notEmpty(rootPath);
        if (!rootPath.startsWith("/") || rootPath.endsWith("/")) {
            String message = String.format("Invalid election root path: %s", rootPath);
            logger.error(message);
            throw new RuntimeException(message);
        }

        this.connection = connection;
        this.electionNodes = new ConcurrentHashMap<String, Boolean>();
        this.rootPath = rootPath;
        this.listeners = new HashSet<ZooKeeperElectionListener>();
    }

    public void addListener(ZooKeeperElectionListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Returns the path to the root node for this election.
     *
     * @return election root path
     */
    public String getRootPath() {
        return this.rootPath;
    }

    /**
     * Enrolls in the election.
     * <p/>
     * This method looks for an existing member node based on the current ZK session.
     * A new member node is always created and then a watch is set on the root node.
     */
    public void enroll() {
        this.enrolled = true;
        withdrawAndEnroll(null, "Client enroll() Request");
    }

    /**
     * Withdraws from the election.
     * <p/>
     * If currently in the {@code LEADER} state, calling this method allows another participant to become the leader.
     */
    public void withdraw() {
        this.enrolled = false;
        withdrawInternal(this.electionContext, "Client withDraw() Request");
    }

    private boolean checkActive(ZooKeeperElectionContext ctx) {
        if (!ctx.isActive()) {
            return false;
        }

        if (electionContext != ctx) {
            ctx.setMaster(false);
            withdrawInternal(ctx, "Active Check Failed");
            return false;
        }

        return true;
    }

    /**
     * Starts a new enrollment, but withdraws from any existing enrollments first.
     *
     * @param previousContext a previous enrollment to withdraw from.
     */
    private void withdrawAndEnroll(ZooKeeperElectionContext previousContext, String reason) {

        if (previousContext != null && previousContext != this.electionContext) {
            withdrawInternal(previousContext, String.format("%s - (PARAM)", reason));
        }

        // If there was already an election running withdraw from it.
        withdrawInternal(this.electionContext, String.format("%s - (STATE)", reason));

        enrollInternal();
    }

    /**
     * Creates the node to actually enroll in the election.
     */
    private void enrollInternal() {
        if (!enrolled) {
            return;
        }

        logger.debug("Enrolling in election");

        // Create a unique base path containing a UUID.
        final String nodePath = rootPath + "/" + ELECTION_NODE_PREFIX + UUID.randomUUID().toString() + "-";
        final ZooKeeperElectionContext ctx = new ZooKeeperElectionContext(this, nodePath);

        // Set the active context
        this.electionContext = ctx;

        // Create a node for this election.
        electionNodes.put(nodePath, Boolean.TRUE);
        connection.createEphemeralSequential(nodePath, ZERO_BYTE_ARRAY, callbackCreatePrimary, ctx);
    }

    /**
     * Called back after an enrollment and the primary node is created.
     */
    void processResultCreatePrimary(int rc, String path, ZooKeeperElectionContext ctx, String name) {
        if (rc != 0) {
            ctx.initializationFailed();
            error("Failed to create primary election node", rc, path, ctx);
            return;
        }

        try {
            ctx.activate(name);
        } catch (IllegalArgumentException e) {
            error("Invalid path to primary election node: " + name, Code.NONODE, path, ctx);
            return;
        }

        if (logger.isInfoEnabled()) {
            logger.info(String.format("Enrolled as node: %s", name));
        }

        listChildren(ctx);
    }

    /**
     * Whenever we think we might be the master list the children to confirm for sure.
     */
    private void listChildren(ZooKeeperElectionContext ctx) {
        // memberPath is null after withdraw
        if (checkActive(ctx)) {
            connection.getChildren(rootPath, null, callbackRootChildren, ctx);
        }
    }

    /**
     * Performs the check to confirm (absolutely that we are the master).
     */
    void processResultRootChildren(int rc, String path, ZooKeeperElectionContext ctx, List<String> children) {
        if (rc != 0) {
            error("Failed to list children of root node", rc, path, ctx);
            return;
        }

        if (!checkActive(ctx)) {
            return;
        }

        cleanupPriorExecution(ctx, path, children);

        final String memberName = ctx.getName();

        // Make sure all the children are sorted by time
        Collections.sort(children, SEQUENTIAL_COMPARATOR);

        final int memberIndex = children.indexOf(memberName);

        ctx.setMaster(memberIndex == 0);

        if (memberIndex == -1) {
            // Since we just added it our node should be present!!
            error("Election node not found: " + memberName, Code.NONODE, path, ctx);
            return;
        }

        if (memberIndex > 0) {
            final String previousName = children.get(memberIndex - 1);
            final String previousPath = rootPath + "/" + previousName;

            if (logger.isInfoEnabled()) {
                logger.info(String.format(
                        "Waiting for previous node: %s, Current Index: %d", previousName, memberIndex));
            }

            // Wait for the previous node to be deleted.
            // Using getData() instead of exists() because I don't want a NodeCreated event.
            connection.getData(
                    previousPath, ctx.createWatcher(callbackPreviousWatcher), callbackPreviousData, ctx);
        }

        connection.getData(ctx.getPath(), ctx.createWatcher(callbackCurrentWatcher), callbackCurrentData, ctx);
    }

    private void cleanupPriorExecution(ZooKeeperElectionContext ctx, String parentPath, List<String> children) {
        for (final String child : children) {
            cleanupPriorElectionNode(ctx, parentPath + "/" + child);
        }
    }

    private void cleanupPriorElectionNode(ZooKeeperElectionContext ctx, String path) {
        final int idx = path.lastIndexOf('-');
        if (idx == -1) {
            return;
        }

        final String basePath = path.substring(0, idx + 1);
        if (electionNodes.containsKey(basePath) && !basePath.equals(ctx.getBasePath())) {
            connection.delete(path, -1, callbackDeletePriorNode, ctx);
        }
    }

    private void processResultDeletePriorNode(int rc, String path, ZooKeeperElectionContext ctx) {
        if (Code.get(rc) == Code.OK) {
            electionNodes.remove(path);

            if (logger.isInfoEnabled()) {
                logger.info(String.format("Deleted (cleaned up) previous election node: %s", path));
            }
            listChildren(ctx);
        }
    }

    private void processResultCurrentData(int rc, String path, ZooKeeperElectionContext ctx) {
        if (!checkActive(ctx)) {
            return;
        }

        switch (Code.get(rc)) {
            case NONODE:
                // Our node got blown away
                withdrawAndEnroll(ctx, "Current Node Deleted before Watch");

                break;
            case OK:
                // Do nothing - waiting for the node to be deleted (the watch will be called).
                break;
            default:
                error("Failed to set watch on previous node", rc, path, ctx);
                break;
        }
    }

    private void processCurrentWatcher(ZooKeeperElectionContext ctx, WatchedEvent event) {

        final KeeperState connectionState = event.getState();
        if (connectionState == KeeperState.SyncConnected) {

            // Has someone blown our node away ?
            if (event.getType() == EventType.NodeDeleted) {
                // Withdraw from this election and sign up again.
                if (ctx.isActive()) {
                    withdrawAndEnroll(ctx, "Current Node Deleted");
                }
            }
        } else {
            error("Connection lost - Primary watch: " + connectionState, Code.CONNECTIONLOSS, "/", ctx);
            withdrawInternal(ctx, "Current Node Connection Lost");
        }
    }

    /**
     * Most of the time this is callback is pointless, it only exists for the NONODE corner case.
     */
    void processResultPreviousData(int rc, String path, ZooKeeperElectionContext ctx) {

        if (!checkActive(ctx)) {
            return;
        }

        switch (Code.get(rc)) {
            case NONODE:
                // This is the important case the node may be deleted between the getChildren() and getData() calls.
                if (electionContext == ctx && ctx.isActive()) {
                    // But we need to be sure so lets get absolute confirmation we are master.
                    listChildren(ctx);
                }
                break;
            case OK:
                // Do nothing - waiting for the node to be deleted (the watch will be called).
                break;
            default:
                error("Failed to set watch on previous node", rc, path, ctx);
                break;
        }
    }

    /**
     * Called when the previous node dies, but we still need to check because the previous node may not be the master.
     */
    void processPreviousWatcher(ZooKeeperElectionContext ctx, WatchedEvent event) {
        // Are we still connected?
        final KeeperState connectionState = event.getState();

        if (connectionState == KeeperState.SyncConnected) {
            // Note: I am expecting a NodeDeleted event here.
            //       But, I need to re-establish the watch in all circumstances anyway.

            // We are probably the master, but we need to confirm in case the previous node was not the master.
            listChildren(ctx);
        } else {
            error("Connection lost - Previous watch: " + connectionState, Code.CONNECTIONLOSS, "/", ctx);
            // Can't do anything so just withdraw.
            withdrawInternal(ctx, "Previous Node Connection Lost");
        }
    }

    private synchronized void withdrawInternal(ZooKeeperElectionContext ctx, String reason) {
        if (ctx == null) {
            return;
        }

        logger.debug(String.format("Withdraw Reason: %s", reason));

        if (ctx.isActive()) {
            // Using local variable to avoid synchronization.
            final String path = ctx.getPath();
            connection.delete(path, -1, null, null);
            ctx.expire();

            logger.debug("Election withdrawal complete: " + path);
        }

        if (this.electionContext == null) {
            // Sometimes multiple calls to de-activate occur - just log it.
            logger.debug("Withdraw: No Active Election");
        } else if (this.electionContext == ctx) {
            this.electionContext = null;
            logger.debug("Withdraw: Election De-activated");
        } else {
            // This is probably a bug (such as multiple enrollments).
            logger.warn("Withdraw: Incorrect Election (Withdraw Attempt)");
        }
    }

    /**
     * Processes unhandled errors.
     *
     * @param message description of what went wrong.
     * @param rc      result code
     * @param path    node path
     */
    private void error(String message, int rc, String path, ZooKeeperElectionContext context) {
        error(message, Code.get(rc), path, context);
    }

    private void error(String message, Code code, String path, ZooKeeperElectionContext context) {
        // Don't bother generating a stack trace if the connection is lost.
        final KeeperException exception = code == Code.CONNECTIONLOSS ? null : KeeperException.create(code);

        logger.error(String.format("%s [path = %s]", message, path), exception);
        withdrawInternal(context, String.format("Error: %s [path = %s]", message, path));
    }

    public void onElectionStateChange(boolean master) {
        if (logger.isInfoEnabled()) {
            logger.info(String.format("Election State Change: %s", master ? "Master" : "Standby"));
        }

        for (ZooKeeperElectionListener listener : listeners) {
            listener.onElectionStateChange(this, master);
        }
    }
}
