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

/**
 * Interface for ZooKeeperElection listeners.
 *
 * @author David Trott
 */
public interface ZooKeeperElectionListener {

    /**
     * Notification of election as leader.
     *
     * @param election the election that sent the notification.
     * @param master   if the current node is the master.
     */
    public void onElectionStateChange(ZooKeeperElection election, boolean master);
}
