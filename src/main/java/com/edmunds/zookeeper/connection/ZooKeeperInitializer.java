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

/**
 * Interface for ZooKeeper connection initializers.
 * <p/>
 * A ZooKeeperInitializer is an operation that is executed whenever a {@code ZooKeeperConnection} connects to a
 * ZooKeeper server (i.e. when it transitions to the {@link ZooKeeperConnectionState#CONNECTED} state). The connection
 * notifies listeners that the state has changed to {@link ZooKeeperConnectionState#INITIALIZED} after all initializers
 * have been executed.
 * <p/>
 * Initializers are useful in establishing a baseline condition on the server, especially among a group of classes that
 * share a  {@code ZooKeeperConnection}. All registered initializers are executed before any connection listener
 * receives the {@code INITIALIZED} event, so all listeners share a common initialized state. This allows one class to
 * contribute initializers that are required for the correct operation of another class.
 * <p/>
 * Use {@link ZooKeeperConnection#addInitializer(ZooKeeperInitializer)} to register initializers with a connection.
 * <p/>
 * Note: Initializers may be executed any number of times within a single ZooKeeperConnection. As such, they should make
 * no assumptions about ZooKeeper state and should safely handle re-execution (including recovery after a previous
 * failure). For example, an initializer that creates nodes should handle the existence or partial creation of nodes
 * from a previous run.
 *
 * @author Ryan Holmes
 */
public interface ZooKeeperInitializer {

    /**
     * Sets the ZooKeeper connection to which this initializer is associated.
     *
     * @param connection the ZooKeeper connection
     */
    public void setConnection(ZooKeeperConnection connection);

    /**
     * Starts the initializer.
     */
    public void start();
}
