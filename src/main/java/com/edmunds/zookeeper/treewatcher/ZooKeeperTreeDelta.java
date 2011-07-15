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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.commons.lang.Validate;

import java.util.Arrays;
import java.util.Map;

import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDelta.Walk.LEAF_FIRST;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDelta.Walk.ROOT_FIRST;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDelta.Walk.ROOT_ONLY;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.DELETE;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.INSERT;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.UPDATE;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;

/**
 * Class implements logic to apply a delta between two {@link ZooKeeperTreeState}'s.
 * <p/>
 * The constructor takes a ZooKeeperTreeDeltaCallback which will be called once for each call to treeConsistent(),
 * if a change is detected.
 */
public class ZooKeeperTreeDelta implements ZooKeeperTreeConsistentCallback {

    /**
     * The callback to invoke if a change is detected.
     */
    private final ZooKeeperTreeDeltaCallback callback;
    /**
     * The primary walk order (the order the main loop walks the nodes).
     */
    private final Walk walkPrimary;

    /**
     * The walk order that should be used for the sub-tree under an inserted node.
     */
    private final Walk walkInserted;
    /**
     * The walk order that should be used for the sub-tree under a deleted node.
     */
    private final Walk walkDeleted;

    /**
     * Defines a list of acceptable walk orders for sub-trees.
     */
    public enum Walk {
        /**
         * Do not walk the sub-tree simple visit the root.
         */
        ROOT_ONLY,
        /**
         * Start at the root an walk out to the leaves.
         */
        ROOT_FIRST,

        /**
         * Start with the leaves than walk back towards the root.
         */
        LEAF_FIRST
    }

    /**
     * Lightweight constructor.
     * <p/>
     * Similar to new ZooKeeperTreeDeltaCallback(callback, ROOT_FIRST, ROOT_ONLY, ROOT_ONLY);
     *
     * @param callback the callback to call when a tree change is detected.
     */
    public ZooKeeperTreeDelta(
            ZooKeeperTreeDeltaCallback callback) {
        this(callback, ROOT_FIRST, ROOT_ONLY, ROOT_ONLY);
    }

    /**
     * Main constructor.
     *
     * @param callback     the callback to call when a tree change is detected.
     * @param walkPrimary  the primary order that the tree should be walked (must be ROOT_FIRST or LEAF_FIRST).
     * @param walkInserted the order that should be used to walk sub-trees under inserted nodes.
     * @param walkDeleted  the order that should be used to walk sub-trees under deleted nodes.
     */
    public ZooKeeperTreeDelta(
            ZooKeeperTreeDeltaCallback callback, Walk walkPrimary,
            Walk walkInserted, Walk walkDeleted) {

        Validate.notNull(callback);
        Validate.notNull(walkPrimary);
        Validate.notNull(walkInserted);
        Validate.notNull(walkDeleted);
        Validate.isTrue(walkPrimary != ROOT_ONLY, "Primary walk cannot be ROOT_ONLY");

        this.callback = callback;
        this.walkPrimary = walkPrimary;
        this.walkInserted = walkInserted;
        this.walkDeleted = walkDeleted;
    }

    /**
     * Called when the tree is consistent.
     *
     * @param oldRoot the previous root node from the previous call.
     * @param newRoot the root of the current tree.
     */
    @Override
    public void treeConsistent(
            ZooKeeperTreeNode oldRoot, ZooKeeperTreeNode newRoot) {

        final ZooKeeperTreeDeltaResult result = new ZooKeeperTreeDeltaResult();

        // Is this the first call ?
        if (oldRoot == null) {
            addNode(walkInserted, INSERT, newRoot, result);
        } else {
            walkTree(oldRoot, newRoot, result);
        }

        if (!result.isEmpty()) {
            callback.treeChanged(result);
        }
    }

    private void walkTree(ZooKeeperTreeNode oldNode, ZooKeeperTreeNode newNode, ZooKeeperTreeDeltaResult result) {

        final ImmutableMap<String, ZooKeeperTreeNode> oldChildren = oldNode.getChildren();
        final ImmutableMap<String, ZooKeeperTreeNode> newChildren = newNode.getChildren();

        final ImmutableSet<String> oldKeys = oldChildren.keySet();
        final ImmutableSet<String> newKeys = newChildren.keySet();

        // Special case if the user has asked for a leaf first walk.
        if (walkPrimary == LEAF_FIRST) {
            walkChildren(oldChildren, newChildren, result);
        }

        // Calculate which nodes have been inserted.
        for (ZooKeeperTreeNode node : Maps.filterKeys(newChildren, not(in(oldKeys))).values()) {
            addNode(walkInserted, INSERT, node, result);
        }

        // The primary walk will change the order for updates so always use ROOT_ONLY
        if (!Arrays.equals(oldNode.getData(), newNode.getData())) {
            addNode(ROOT_ONLY, UPDATE, newNode, result);
        }

        // Calculate the deleted nodes.
        for (ZooKeeperTreeNode node : Maps.filterKeys(oldChildren, not(in(newKeys))).values()) {
            addNode(walkDeleted, DELETE, node, result);
        }

        // This is the normal case.
        if (walkPrimary == ROOT_FIRST) {
            walkChildren(oldChildren, newChildren, result);
        }
    }

    private void walkChildren(
            ImmutableMap<String, ZooKeeperTreeNode> oldChildren,
            ImmutableMap<String, ZooKeeperTreeNode> newChildren,
            ZooKeeperTreeDeltaResult result) {

        for (Map.Entry<String, ZooKeeperTreeNode> entry : newChildren.entrySet()) {
            final ZooKeeperTreeNode oldValue = oldChildren.get(entry.getKey());
            if (oldValue != null) {
                walkTree(oldValue, entry.getValue(), result);
            }
        }
    }

    // Adds a sub-tree to the result object.

    private void addNode(Walk walk, Type type, ZooKeeperTreeNode node, ZooKeeperTreeDeltaResult result) {
        if (walk == Walk.LEAF_FIRST) {
            addChildren(walk, type, node, result);
        }

        result.addNode(type, node);

        if (walk == Walk.ROOT_FIRST) {
            addChildren(walk, type, node, result);
        }
    }

    private void addChildren(Walk walk, Type type, ZooKeeperTreeNode node, ZooKeeperTreeDeltaResult result) {
        for (ZooKeeperTreeNode child : node.getChildren().values()) {
            addNode(walk, type, child, result);
        }
    }
}
