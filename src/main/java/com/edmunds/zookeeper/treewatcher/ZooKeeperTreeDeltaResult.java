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

import com.google.common.collect.Maps;
import org.apache.commons.lang.Validate;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.DELETE;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.INSERT;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.UPDATE;

/**
 * Contains the result of a delta operation.
 */
public class ZooKeeperTreeDeltaResult {

    /**
     * Mapping from the type to the list of nodes that have been inserted, updated or deleted.
     */
    private final Map<Type, List<ZooKeeperTreeNode>> nodes;

    /**
     * Default constructor that initializes the node mapping with tree empty lists..
     */
    public ZooKeeperTreeDeltaResult() {
        this.nodes = Maps.newHashMap();
        this.nodes.put(INSERT, new ArrayList<ZooKeeperTreeNode>());
        this.nodes.put(UPDATE, new ArrayList<ZooKeeperTreeNode>());
        this.nodes.put(DELETE, new ArrayList<ZooKeeperTreeNode>());
    }

    /**
     * Returns the list of nodes of the given type.
     *
     * @param type the type of the node.
     * @return the list of nodes (or an empty list if no nodes of that type exist).
     */
    public List<ZooKeeperTreeNode> getNodes(Type type) {
        Validate.notNull(type);

        return nodes.get(type);
    }

    /**
     * Returns the list of inserted nodes.
     *
     * @return the inserted nodes.
     */
    public List<ZooKeeperTreeNode> getInsertedNodes() {
        return getNodes(INSERT);
    }

    /**
     * Returns the list of nodes where the value of the data has been updated.
     *
     * @return the updated nodes.
     */
    public List<ZooKeeperTreeNode> getUpdatedNodes() {
        return getNodes(UPDATE);
    }

    /**
     * Returns the list of deleted nodes.
     *
     * @return the deleted nodes.
     */
    public List<ZooKeeperTreeNode> getDeletedNodes() {
        return getNodes(DELETE);
    }

    /**
     * Returns true if no the result object contains no changes.
     *
     * @return true is all three lists are empty.
     */
    public boolean isEmpty() {
        return getInsertedNodes().isEmpty() && getUpdatedNodes().isEmpty() && getDeletedNodes().isEmpty();
    }

    /**
     * Adds a node of the given type to the internal list.
     *
     * @param type the type of the node.
     * @param node the node to be added.
     */
    void addNode(Type type, ZooKeeperTreeNode node) {
        Validate.notNull(type);
        Validate.notNull(node);

        getNodes(type).add(node);
    }

    /**
     * Enumeration of the lists of nodes.
     */
    public enum Type {
        INSERT,
        UPDATE,
        DELETE
    }
}
