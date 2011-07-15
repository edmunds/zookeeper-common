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
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Elects a leader among a group of coordinated processes.
 * <p/>
 * Each participant in an election begins by calling the {@link #enroll(ZooKeeperElectionListener)} method. A sequential
 * ephemeral node is created in the election root path to represent the participant. At this point they are considered
 * "enrolled" in the election. The participant whose node has the lowest seqeunce number becomes the leader and is
 * notified via the {@link ZooKeeperElectionListener#onElectionLeader(ZooKeeperElection)} method.
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
 * <p/>
 * When the previous node p is deleted:
 * 1. Fetch the children of the root path.
 * 2. If z has the lowest sequence number in the set of children, notify the listener that we are the leader.
 * 3. Otherwise, find a new previous node p and watch for changes.
 * </pre>
 *
 * @author Ryan Holmes
 */
public class ZooKeeperElection {
    private static final Logger logger = Logger.getLogger(ZooKeeperElection.class);
    private static final String ELECTION_NODE_PREFIX = "node-";
    private static final List<Code> retryableErrors = Arrays.asList(
        Code.RUNTIMEINCONSISTENCY,
        Code.DATAINCONSISTENCY,
        Code.CONNECTIONLOSS,
        Code.MARSHALLINGERROR,
        Code.OPERATIONTIMEOUT);

    private final ZooKeeperConnection connection;
    private final String rootPath;

    private ZooKeeperElectionListener electionListener;

    private ElectionState electionState;
    private String electionNodeName;

    private Watcher previousNodeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            onPreviousNodeEvent(event);
        }
    };

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
        this.rootPath = rootPath;
        this.electionState = ElectionState.READY;
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
     * If currently in the {@code READY} or {@code WITHDRAWN} state, a new election node will be created. Otherwise, the
     * election procedure is resumed by checking for a previous node.
     *
     * @param listener the election listener
     * @throws IllegalArgumentException if the listener is {@code null}
     */
    public void enroll(ZooKeeperElectionListener listener) throws IllegalArgumentException {

        Validate.notNull(listener, "An election listener must be specified");
        electionListener = listener;

        logger.debug("Enrolling in election");

        switch (electionState) {
            case READY:
            case WITHDRAWN:
                // Create a new election node
                electionState = ElectionState.ENROLLED;
                createElectionNode();
                break;
            case ENROLLED:
            case LEADER:
                // Election node exists, re-acquire the previous node
                logger.warn(String.format("Enrolling in election while in state %s", electionState));
                findPreviousNode();
                break;
            default:
                throw new IllegalStateException(String.format("Unknown election state: %s", electionState));
        }
    }

    /**
     * Withdraws from the election.
     * <p/>
     * If currently in the {@code LEADER} state, calling this method allows another participant to become the leader.
     * The listener will notified of successful withdrawal via the {@link ZooKeeperElectionListener#onElectionWithdrawn(ZooKeeperElection)}
     * method.
     */
    public void withdraw() {

        logger.debug("Withdrawing from election");

        switch (electionState) {
            case ENROLLED:
            case LEADER:
                // Delete the existing election node
                electionState = ElectionState.WITHDRAWN;
                deleteElectionNode();
                break;
            case READY:
            case WITHDRAWN:
                // Election node not yet created or in process of being deleted, retry delete anyway
                // in case we were called as a result of error handling.
                logger.warn(String.format("Withdrew from election while in state %s", electionState));
                deleteElectionNode();
                break;
            default:
                throw new IllegalStateException(String.format("Unknown election state: %s", electionState));
        }
    }

    /**
     * Returns true if the election is in {@link ElectionState#READY} state.
     *
     * @return true if ready for enrollment, false otherwise
     */
    public boolean isReady() {
        return electionState == ElectionState.READY;
    }

    /**
     * Returns true if the election is in {@link ElectionState#ENROLLED} state.
     *
     * @return true if enrolled in the election, false otherwise
     */
    public boolean isEnrolled() {
        return electionState == ElectionState.ENROLLED;
    }

    /**
     * Returns true if the election is in {@link ElectionState#LEADER} state.
     *
     * @return true if leadership has been acquired, false otherwise
     */
    public boolean isLeader() {
        return electionState == ElectionState.LEADER;
    }

    /**
     * Returns true if the election is in {@link ElectionState#WITHDRAWN} state.
     *
     * @return true if withdrawn from the election, false otherwise
     */
    public boolean isWithdrawn() {
        return electionState == ElectionState.WITHDRAWN;
    }

    /**
     * Creates an election member node.
     */
    private void createElectionNode() {

        // Create sequential ephemeral election node
        AsyncCallback.StringCallback cb = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                onElectionNodeCreated(Code.get(rc), path, name);
            }
        };
        String path = rootPath + "/" + ELECTION_NODE_PREFIX;
        connection.createEphemeralSequential(path, new byte[0], cb, null);
    }

    /**
     * Handles the result of creating an election member node.
     *
     * @param rc   result code
     * @param path the requested node path
     * @param name the actual name of the created node
     */
    private void onElectionNodeCreated(Code rc, String path, String name) {

        if (rc == Code.OK) {
            if (logger.isDebugEnabled()) {
                String message = String.format("Election node created: %s", name);
                logger.debug(message);
            }
            // Enrolled in election
            enrollmentComplete(name);
        } else {
            error(rc, path);
        }
    }

    /**
     * Finds and watches the previous election member node.
     */
    private void findPreviousNode() {

        logger.debug("Finding previous node");

        // Get election children
        AsyncCallback.Children2Callback cb = new AsyncCallback.Children2Callback() {
            @Override
            public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
                onElectionChildrenReceived(Code.get(rc), path, children);
            }
        };
        connection.getChildren(rootPath, null, cb, null);
    }

    /**
     * Called when election members have been received as a result of {@link #findPreviousNode()}.
     *
     * @param rc       result code
     * @param path     parent node path
     * @param children election member node names
     */
    private void onElectionChildrenReceived(Code rc, String path, List<String> children) {

        if (rc == Code.OK) {
            // Sort the children so we don't assume a specific order from ZooKeeper
            Collections.sort(children);

            // Find the previous node
            int pos = children.indexOf(electionNodeName);
            if (pos > 0) {

                // Found previous node
                watchPreviousNode(children.get(pos - 1));
            } else if (pos == 0) {

                // No previous node, we are the leader
                leadershipAcquired();
            } else {

                // Our node was not found, re-enroll in the election
                reenroll();
            }
        } else {
            error(rc, path);
        }
    }

    /**
     * Places an 'exists' watch on the previous node.
     *
     * @param nodeName name of the previous node
     */
    private void watchPreviousNode(String nodeName) {

        if (logger.isDebugEnabled()) {
            String message = String.format("Setting watch on previous node: %s", nodeName);
            logger.debug(message);
        }

        // Set a watch on the previous node
        AsyncCallback.StatCallback existsCallback = new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                onPreviousNodeExists(Code.get(rc), path);
            }
        };

        String path = rootPath + "/" + nodeName;
        connection.exists(path, previousNodeWatcher, existsCallback, null);
    }

    /**
     * Handles the result of the exists call on the previous node.
     *
     * @param rc   result code
     * @param path node path
     */
    private void onPreviousNodeExists(Code rc, String path) {

        switch (rc) {
            case OK:
                // Previous node exists
                if (logger.isDebugEnabled()) {
                    String message = String.format("Previous node exists: %s", path);
                    logger.debug(message);
                }
                break;
            case NONODE:
                // Previous node deleted, retry
                if (logger.isDebugEnabled()) {
                    String message = String.format("Previous node does not exist: %s", path);
                    logger.debug(message);
                }
                findPreviousNode();
                break;
            default:
                error(rc, path);
        }
    }

    /**
     * Handles a watch event on the previous node.
     *
     * @param event the watch event
     */
    private void onPreviousNodeEvent(WatchedEvent event) {

        if (event.getType() == Watcher.Event.EventType.NodeDeleted) {

            if (logger.isDebugEnabled()) {
                String message = String
                    .format("Previous node deleted: %s", event.getPath());
                logger.debug(message);
            }

            // Look for a new previous node, don't assume we are the leader
            findPreviousNode();
        }
    }

    /**
     * Deletes our member node from the election root path.
     */
    private void deleteElectionNode() {

        AsyncCallback.VoidCallback cb = new AsyncCallback.VoidCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx) {
                onElectionNodeDeleted(Code.get(rc), path);
            }
        };

        if (electionNodeName != null) {
            connection.delete(rootPath + "/" + electionNodeName, -1, cb, null);
        } else {
            withdrawalComplete();
        }
    }

    /**
     * Handles the deletion of our election node.
     *
     * @param rc   result code
     * @param path node path
     */
    private void onElectionNodeDeleted(Code rc, String path) {

        switch (rc) {
            case OK:
                if (logger.isDebugEnabled()) {
                    String message = String.format("Deleted election node: %s", path);
                    logger.debug(message);
                }
                withdrawalComplete();
                break;
            case NONODE:
                logger.warn(String.format("Election node deleted unexpectedly: %s", path));
                withdrawalComplete();
                break;
            default:
                if (isRetryableError(rc)) {
                    deleteElectionNode();
                } else {
                    error(rc, path);
                }
        }
    }

    /**
     * Called upon successful enrollment in the election.
     * <p/>
     * This method updates our election node name and searches for a previous member node.
     *
     * @param name name of the created member node
     */
    private void enrollmentComplete(String name) {

        if (electionState != ElectionState.ENROLLED) {
            logger.warn(String.format("Completed election enrollment while in state %s", electionState));
            electionState = ElectionState.ENROLLED;
        }

        electionNodeName = name.substring(name.lastIndexOf("/") + 1);
        Validate.notNull(electionNodeName);
        findPreviousNode();
    }

    /**
     * Called when leadership has been acquired.
     * <p/>
     * This method transitions to the {@code LEADER} state and notifies the listener.
     */
    private void leadershipAcquired() {

        if (electionState != ElectionState.ENROLLED) {
            logger.warn(String.format("Elected as leader while in state %s", electionState));
        }

        if (logger.isDebugEnabled()) {
            logger.debug(String.format("Elected as leader with node: %s", electionNodeName));
        }

        electionState = ElectionState.LEADER;
        electionListener.onElectionLeader(this);
    }

    /**
     * Called when withdrawal from the election is complete.
     * <p/>
     * This method transitions to the {@code READY} state and notifies the listener.
     */
    private void withdrawalComplete() {

        if (electionState != ElectionState.WITHDRAWN) {
            logger.warn(String.format("Withdrawal completed while in state %s", electionState));
        }

        logger.debug("Election withdrawal complete");
        electionNodeName = null;
        electionState = ElectionState.READY;
        electionListener.onElectionWithdrawn(this);
    }

    /**
     * Re-enrolls in the election.
     * <p/>
     * This is a recovery method for use when our member node is unexpectedly missing.
     */
    private void reenroll() {
        logger.warn(String.format("Re-enrolling while in state %s", electionState));

        // Withdraw from the election if we are currently enrolled or the leader
        if (electionState == ElectionState.ENROLLED || electionState == ElectionState.LEADER) {
            electionState = ElectionState.WITHDRAWN;
            withdrawalComplete();
        }

        // Re-enroll
        enroll(electionListener);
    }

    /**
     * Processes unhandled errors.
     *
     * @param rc   result code
     * @param path node path
     */
    private void error(Code rc, String path) {
        KeeperException ex = KeeperException.create(rc, path);
        logger.error(ex);
        electionListener.onElectionError(this, ex);
    }

    /**
     * Returns true if the given error is temporary and may be retried.
     *
     * @param rc result code
     * @return true if error may be retried, false otherwise
     */
    private boolean isRetryableError(Code rc) {
        return retryableErrors.contains(rc);
    }
}
