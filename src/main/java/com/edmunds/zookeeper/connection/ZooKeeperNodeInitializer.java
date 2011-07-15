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

import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.data.Stat;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * A ZooKeeperInitializer that creates one or more persistent nodes.
 * <p/>
 * Note: This class creates parent nodes as needed.
 *
 * @author Ryan Holmes
 */
public class ZooKeeperNodeInitializer implements ZooKeeperInitializer {

    private static final Logger logger = Logger.getLogger(ZooKeeperNodeInitializer.class);
    private static final List<Code> retryableErrors = Arrays.asList(
            Code.RUNTIMEINCONSISTENCY,
            Code.DATAINCONSISTENCY,
            Code.CONNECTIONLOSS,
            Code.MARSHALLINGERROR,
            Code.OPERATIONTIMEOUT);

    private final Collection<String> paths;
    private final LinkedList<String> pathQueue = new LinkedList<String>();
    private ZooKeeperConnection connection;

    /**
     * Constructs a new ZooKeeperNodeInitializer for the given node paths.
     *
     * @param paths node paths to create
     */
    public ZooKeeperNodeInitializer(String... paths) {
        this(Arrays.asList(paths));
    }

    /**
     * Constructs a new ZooKeeperNodeInitializer for the given collection of node paths.
     *
     * @param paths node paths to create
     */
    public ZooKeeperNodeInitializer(Collection<String> paths) {
        this.paths = paths;
    }

    @Override
    public void setConnection(ZooKeeperConnection connection) {
        this.connection = connection;
    }

    @Override
    public void start() {
        if (!paths.isEmpty()) {
            pathQueue.clear();
            pathQueue.addAll(paths);
            createNextPath();
        }
    }

    private void createNextPath() {

        AsyncCallback.StatCallback cb = new AsyncCallback.StatCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, Stat stat) {
                onPathExists(Code.get(rc), path);
            }
        };

        if (pathQueue.isEmpty()) {

            // Finished creating paths
            connection.initializerFinished(this);
        } else {

            // Check if next path exists
            String path = pathQueue.getFirst();
            connection.exists(path, null, cb, null);
        }
    }

    private void onPathExists(Code rc, String path) {
        AsyncCallback.StringCallback cb = new AsyncCallback.StringCallback() {
            @Override
            public void processResult(int rc, String path, Object ctx, String name) {
                onPathCreated(Code.get(rc), path);
            }
        };

        if (rc == Code.NONODE) {
            // Path does not exist
            connection.createPersistentWithParents(path, null, cb, null);
        } else {
            // Path exists or error
            onPathCreated(rc, path);
        }
    }

    private void onPathCreated(Code rc, String path) {

        if (rc == Code.OK || rc == Code.NODEEXISTS) {

            // Path created or already exists
            if (logger.isDebugEnabled()) {
                String message = String.format("Initialized path %s with result %s", path, rc);
                logger.debug(message);
            }

            // Create next path
            pathQueue.remove(path);
            createNextPath();
        } else if (isRetryableError(rc)) {

            // Retry system errors
            String message = String.format("Creation of path %s failed with error %s, retrying", path, rc);
            logger.warn(message);
            createNextPath();
        } else {

            // Unrecoverable error
            String message = String.format("Creation of path %s failed with error %s", path, rc);
            logger.error(message);
            throw new RuntimeException(message);
        }
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
