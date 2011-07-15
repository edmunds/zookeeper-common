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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Wrapper around the ZooKeeperTreeNode's.
 * <p/>
 * The tree nodes are immutable so this class provides (mutable) maps to quickly look up elements in the tree
 * and methods to perform tree manipulation.
 */
public class ZooKeeperTreeState {

    private static final Logger logger = Logger.getLogger(ZooKeeperTreeState.class);

    private static final Set<ZooKeeperTreeNode> EMPTY_SET = Collections.emptySet();
    private static final ImmutableMap<String, ZooKeeperTreeNode> EMPTY_MAP = ImmutableMap.of();

    /**
     * Maps the path to the actual node.
     */
    private final Map<String, ZooKeeperTreeNode> pathMapping;

    /**
     * Allows each node to lookup its parent.
     * <p/>
     * This is important, since there are no parent references in the tree.
     */
    private final Map<ZooKeeperTreeNode, ZooKeeperTreeNode> parentMapping;

    /**
     * The node at the root of the tree.
     */
    private ZooKeeperTreeNode rootNode;

    /**
     * Standard constructor that initializes a Tree State with a single root node.
     * <p/>
     * If both the data and the children are consistent the node is assumed to be fully consistent.
     * <p/>
     * Each node in the tree has a "level" typically the root node is level 0 and the level below that is 1 and so on.
     * However it may be the case that logic that needs to process the tree needs the level to have a specific value.
     * As a result it is possible to set the root to any value, the watcher then increments +1 for each level.
     *
     * @param rootLevel           the level of the root node.
     * @param rootPath            the path to the root node.
     * @param data                the data contained in the root node.
     * @param dataConsistent      is the data consistent.
     * @param childListConsistent are the child nodes consistent.
     */

    public ZooKeeperTreeState(
            int rootLevel, String rootPath, byte[] data, boolean dataConsistent, boolean childListConsistent) {

        this.pathMapping = Maps.newHashMap();
        this.parentMapping = Maps.newHashMap();

        setRootNode(new ZooKeeperTreeNode(rootLevel, rootPath, data,
                dataConsistent, childListConsistent, dataConsistent && childListConsistent));
    }

    /**
     * Copy constructor.
     * <p/>
     * Since most of the tree is immutable copying it is a very cheap operation.
     *
     * @param other the other state to copy.
     */
    public ZooKeeperTreeState(ZooKeeperTreeState other) {
        this.pathMapping = Maps.newHashMap(other.pathMapping);
        this.parentMapping = Maps.newHashMap(other.parentMapping);
        this.rootNode = other.rootNode;
    }

    public ZooKeeperTreeNode getRootNode() {
        return rootNode;
    }

    private void setRootNode(ZooKeeperTreeNode rootNode) {
        pathMapping.put(rootNode.getPath(), rootNode);
        this.rootNode = rootNode;
    }

    public ZooKeeperTreeNode getNode(String path) {
        return pathMapping.get(path);
    }

    public void setFlags(String path, boolean dataConsistent, boolean childListConsistent) {
        final ZooKeeperTreeNode oldNode = pathMapping.get(path);

        final ZooKeeperTreeNode newNode = createNode(
                oldNode, oldNode.getChildren(), oldNode.getData(),
                dataConsistent, childListConsistent, true);

        switchParent(oldNode, newNode);
    }

    public void setData(String path, byte[] data) {
        final ZooKeeperTreeNode oldNode = pathMapping.get(path);

        final ZooKeeperTreeNode newNode = createNode(
                oldNode, oldNode.getChildren(), data,
                true, oldNode.isChildListConsistent(), true);

        switchParent(oldNode, newNode);
    }

    public void setChildren(String path, Collection<String> children) {
        final ZooKeeperTreeNode parent = pathMapping.get(path);

        final List<ZooKeeperTreeNode> deletedChildren = getDeletedChildren(parent, children);
        final List<ZooKeeperTreeNode> insertedChildren = getInsertedChildren(parent, children);

        updateNode(
                parent, deletedChildren, insertedChildren,
                parent.isDataConsistent(), true, insertedChildren.isEmpty());
    }

    public void insertNode(String parentPath, ZooKeeperTreeNode insertedNode) {
        insertNodes(pathMapping.get(parentPath), Collections.singleton(insertedNode));
    }

    public void insertNodes(ZooKeeperTreeNode parent, Collection<ZooKeeperTreeNode> insertedNodes) {
        Validate.notNull(parent, "parent cannot be null");
        Validate.notNull(insertedNodes, "node cannot be null");

        for (ZooKeeperTreeNode insertedNode : insertedNodes) {
            if (parent.getChild(insertedNode.getName()) != null) {
                final String msg = "Cannot overwrite existing node: " + insertedNode.getPath();
                logger.error(msg);
                throw new IllegalStateException(msg);
            }
        }
        updateNode(parent, EMPTY_SET, insertedNodes, parent.isDataConsistent(), parent.isChildListConsistent(), true);
    }

    public void deleteNode(String path) {
        Validate.notEmpty(path);

        final ZooKeeperTreeNode node = pathMapping.get(path);
        Validate.notNull(node);

        final ZooKeeperTreeNode parent = parentMapping.get(node);
        Validate.notNull(parent);

        deleteNodes(parent, Collections.singleton(node));
    }

    public void deleteNodes(ZooKeeperTreeNode parent, Collection<ZooKeeperTreeNode> deletedNodes) {
        Validate.notNull(parent, "parent cannot be null");
        Validate.notNull(deletedNodes, "deletedNode cannot be null");

        updateNode(parent, deletedNodes, EMPTY_SET, parent.isDataConsistent(), parent.isChildListConsistent(), true);
    }

    private void updateNode(
            ZooKeeperTreeNode parent,
            Collection<ZooKeeperTreeNode> deletedNodes, Collection<ZooKeeperTreeNode> insertedNodes,
            boolean dataConsistent, boolean childListConsistent, boolean checkConsistent) {

        for (ZooKeeperTreeNode deletedNode : deletedNodes) {
            deleteSubTree(deletedNode);
        }

        for (ZooKeeperTreeNode insertedNode : insertedNodes) {
            // This call will set the wrong parent (we need newParent).
            insertSubTree(parent, insertedNode);
        }

        // Correct the parent of the children during the creation.
        final ZooKeeperTreeNode newParent = createParent(
                parent, deletedNodes, insertedNodes,
                dataConsistent, childListConsistent, checkConsistent);

        switchParent(parent, newParent);
    }

    private void insertSubTree(ZooKeeperTreeNode parent, ZooKeeperTreeNode node) {
        if (!parent.getPath().equals(node.getParentPath())) {
            final String msg = "Child node has an incorrect parent: " +
                    "[" + node.getPath() + "] -> [" + parent.getPath() + "]";
            logger.error(msg);
            throw new IllegalStateException(msg);
        }

        pathMapping.put(node.getPath(), node);
        parentMapping.put(node, parent);

        for (ZooKeeperTreeNode child : node.getChildren().values()) {
            insertSubTree(node, child);
        }
    }

    private void deleteSubTree(ZooKeeperTreeNode node) {
        for (ZooKeeperTreeNode child : node.getChildren().values()) {
            deleteSubTree(child);
        }

        if (pathMapping.remove(node.getPath()) == null) {
            logger.error("Tried to remove a node that is not in the path mapping: " + node.getPath());
        }

        if (parentMapping.remove(node) == null) {
            logger.error("Tried to remove a node that is not in the tree: " + node.getPath());
        }
    }

    private List<ZooKeeperTreeNode> getInsertedChildren(ZooKeeperTreeNode parent, Collection<String> children) {
        final List<ZooKeeperTreeNode> insertedChildren = Lists.newArrayList();
        final int childLevel = parent.getLevel() + 1;
        final String parentPath = parent.getPath();

        for (String child : children) {
            if (parent.getChild(child) == null) {
                insertedChildren.add(
                        new ZooKeeperTreeNode(childLevel, parentPath, child, null, EMPTY_MAP, false, false, false));
            }
        }

        return insertedChildren;
    }

    private List<ZooKeeperTreeNode> getDeletedChildren(ZooKeeperTreeNode parent, Collection<String> children) {
        final List<ZooKeeperTreeNode> deletedChildren = Lists.newArrayList();
        final HashSet<String> childrenSet = Sets.newHashSet(children);

        for (ZooKeeperTreeNode child : parent.getChildren().values()) {
            if (!childrenSet.contains(child.getName())) {
                deletedChildren.add(child);
            }
        }

        return deletedChildren;
    }

    private ZooKeeperTreeNode createParent(
            ZooKeeperTreeNode parent,
            Collection<ZooKeeperTreeNode> deletedNodes, Collection<ZooKeeperTreeNode> insertedNodes,
            boolean dataConsistent, boolean childListConsistent, boolean checkConsistent) {

        final HashMap<String, ZooKeeperTreeNode> children = Maps.newHashMap(parent.getChildren());

        // Remove any deleted nodes.
        for (ZooKeeperTreeNode deletedNode : deletedNodes) {
            children.remove(deletedNode.getName());
        }

        // Add any inserted nodes.
        for (ZooKeeperTreeNode insertedNode : insertedNodes) {
            children.put(insertedNode.getName(), insertedNode);
        }

        return createNode(
                parent, children, parent.getData(),
                dataConsistent, childListConsistent, checkConsistent);
    }

    private ZooKeeperTreeNode createNode(
            ZooKeeperTreeNode oldNode, Map<String, ZooKeeperTreeNode> children, byte[] data,
            boolean dataConsistent, boolean childListConsistent, boolean checkConsistent) {

        boolean fullyConsistent =
                dataConsistent && childListConsistent &&
                        checkConsistent && isFullyConsistent(children.values());

        final ZooKeeperTreeNode newParent = new ZooKeeperTreeNode(
                oldNode,
                data, ImmutableMap.copyOf(children),
                dataConsistent, childListConsistent, fullyConsistent);

        // At this point the parent mappings will be incorrect, we need update to the newly created node.
        for (ZooKeeperTreeNode child : newParent.getChildren().values()) {
            parentMapping.put(child, newParent);
        }

        return newParent;
    }

    private void switchParent(ZooKeeperTreeNode oldNode, ZooKeeperTreeNode newNode) {
        Validate.notNull(oldNode, "oldNode cannot be null");
        Validate.notNull(newNode, "newNode cannot be null");

        // Have we reached the root?
        while (oldNode != getRootNode()) {
            final String path = newNode.getPath();
            final ZooKeeperTreeNode oldValue = pathMapping.put(path, newNode);

            if (oldValue != oldNode) {
                final String msg = "oldNode was not in the pathMapping: " + oldNode.getPath();
                logger.error(msg);
                throw new IllegalStateException(msg);
            }

            final ZooKeeperTreeNode parent = parentMapping.remove(oldNode);

            if (parent == null) {
                final String msg = "oldNode does not have a parent: " + oldNode.getPath();
                logger.error(msg);
                throw new IllegalStateException(msg);
            }

            // Move up one level (towards the root)
            newNode = createParent(
                    parent, Collections.singleton(oldNode), Collections.singleton(newNode),
                    parent.isDataConsistent(), parent.isChildListConsistent(), newNode.isFullyConsistent());
            oldNode = parent;
        }

        setRootNode(newNode);
    }

    private boolean isFullyConsistent(Collection<ZooKeeperTreeNode> children) {
        for (ZooKeeperTreeNode child : children) {
            if (!child.isFullyConsistent()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Helper method to support testing.
     *
     * @param node the node to lookup the parent for.
     * @return the parent or null.
     */
    ZooKeeperTreeNode getParent(ZooKeeperTreeNode node) {
        return parentMapping.get(node);
    }

    int getParentMappingSize() {
        return parentMapping.size();
    }

    int getPathMappingSize() {
        return pathMapping.size();
    }
}
