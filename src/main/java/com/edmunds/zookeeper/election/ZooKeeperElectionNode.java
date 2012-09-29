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

import org.apache.commons.lang.Validate;

/**
 * Represents the known name of a master election node (after the callback from create).
 *
 * @author David Trott
 */
public class ZooKeeperElectionNode implements Comparable<ZooKeeperElectionNode> {

    /**
     * The name of the node (without any path).
     */

    private final String name;

    /**
     * The full path to the node.
     */
    private final String path;

    /**
     * Constructs a ZooKeeperElectionNode from a full path.
     *
     * @param path the path.
     * @throws IllegalArgumentException if the path is blank or doesn't contain a /
     */
    public ZooKeeperElectionNode(String path) throws IllegalArgumentException {
        Validate.notEmpty(path, "path is blank");

        final int lastSlash = path.lastIndexOf("/");
        Validate.isTrue(lastSlash != -1, "path must contain a /");

        this.name = path.substring(lastSlash + 1);
        this.path = path;
    }

    public String getName() {
        return name;
    }

    public String getPath() {
        return path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final ZooKeeperElectionNode that = (ZooKeeperElectionNode) o;

        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public int compareTo(ZooKeeperElectionNode that) {
        if (this == that) {
            return 0;
        }

        return path.compareTo(that.path);
    }
}
