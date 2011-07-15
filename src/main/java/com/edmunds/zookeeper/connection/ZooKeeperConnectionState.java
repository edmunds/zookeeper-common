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
 * Enumeration of possible {@link com.edmunds.zookeeper.connection.ZooKeeperConnection} states.
 * <p/>
 * A {@code ZooKeeperConnection} notifies listeners whenever the connection's state changes.
 *
 * @see com.edmunds.zookeeper.connection.ZooKeeperConnectionListener
 */
public enum ZooKeeperConnectionState {

    /**
     * Connection closed.
     * <p/>
     * This is the initial state of a {@link com.edmunds.zookeeper.connection.ZooKeeperConnection}.
     */
    CLOSED,

    /**
     * Connected or reconnected to a ZooKeeper server.
     * <p/>
     * Note that this state occurs before the {@link #INITIALIZED} state and, therefore, before any initializers have
     * been processed.
     */
    CONNECTED,

    /**
     * All registered initializers completed.
     * <p/>
     * An event for this state is dispatched after all {@link com.edmunds.zookeeper.connection.ZooKeeperInitializer}
     * objects registered with the connection have been completed. Its primary purpose is to provide a known state in
     * which to begin your application's interactions with ZooKeeper (e.g. setting a watch to initiate event-driven
     * code) and/or to recover from a lost connection.
     */
    INITIALIZED,

    /**
     * Disconnected from the ZooKeeper server.
     * <p/>
     * This state occurs when the client loses its connection to the server. This may be a temporary state, and the
     * ZooKeeper client will automatically try to reconnect to the server. A {@link #CONNECTED} event will be dispatched
     * if the reconnection succeeds.
     */
    DISCONNECTED,

    /**
     * The ZooKeeper session has expired.
     * <p/>
     * Session expiration is determined by the server and this method will only be called if the client reconnects to
     * the server after a a period of disconnection longer than the session timeout.
     */
    EXPIRED
}
