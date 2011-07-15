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

import org.apache.zookeeper.KeeperException;

/**
 * Interface for ZooKeeperElection listeners.
 *
 * @author Ryan Holmes
 */
public interface ZooKeeperElectionListener {

    /**
     * Notification of election as leader.
     *
     * @param election the election that sent the notification
     */
    public void onElectionLeader(ZooKeeperElection election);

    /**
     * Notification of successful withdrawal from the election.
     *
     * @param election the election that sent the notification
     */
    public void onElectionWithdrawn(ZooKeeperElection election);

    /**
     * Notification that an unrecoverable ZooKeeper error occurred during the election. The error may represent a
     * deadlock (e.g. member node cannot be deleted) or other fatal error (e.g. root path does not exist).
     *
     * @param election the election that sent the notification
     * @param error    a {@code KeeperException} representing the error
     */
    public void onElectionError(ZooKeeperElection election, KeeperException error);
}
