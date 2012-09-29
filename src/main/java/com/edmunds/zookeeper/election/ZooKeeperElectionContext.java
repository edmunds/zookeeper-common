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

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;

/**
 * Represents the current state of the election each time we re-connect to ZooKeeper.
 *
 * @author David Trott
 */
public class ZooKeeperElectionContext {

    /**
     * States that the context can be in.
     */
    public enum State {
        /**
         * The node has not yet been created in ZooKeeper.
         */
        initializing,

        /**
         * The node is created - everything is normal.
         */
        active,

        /**
         * The election is over - a new election is running.
         */
        expired,

        /**
         * An error occurred while creating the node.
         */
        error
    }

    private final ZooKeeperElection election;

    /**
     * Current state of this context.
     */
    private volatile State state = State.initializing;

    /**
     * The node we have created in this cycle.
     */
    private volatile ZooKeeperElectionNode activeNode;

    private volatile boolean master;

    public ZooKeeperElectionContext(ZooKeeperElection election) {
        this.election = election;
    }

    public State getState() {
        return state;
    }

    public boolean isActive() {
        return this.activeNode != null && state == State.active;
    }

    public String getName() {
        return activeNode.getName();
    }

    public String getPath() {
        return activeNode.getPath();
    }

    public boolean isMaster() {
        return master;
    }

    public void initializationFailed() {
        this.state = State.error;
    }

    public ZooKeeperElectionNode activate(String path) {
        this.activeNode = new ZooKeeperElectionNode(path);
        this.state = State.active;
        return this.activeNode;
    }

    public void expire() {
        this.state = State.expired;
        setMaster(false);
    }

    public void setMaster(boolean master) {
        final boolean stateChanged;
        synchronized (this) {
            stateChanged = this.master != master;
            this.master = master;
        }

        if (stateChanged) {
            election.onElectionStateChange(master);
        }
    }

    /**
     * Creates a watcher that encapsulates this context.
     *
     * @param zooKeeperElectionContextWatcher
     *         the callback.
     * @return the new watcher.
     */
    public Watcher createWatcher(final ZooKeeperElectionContextWatcher zooKeeperElectionContextWatcher) {
        return new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                zooKeeperElectionContextWatcher.process(ZooKeeperElectionContext.this, event);
            }
        };
    }
}
