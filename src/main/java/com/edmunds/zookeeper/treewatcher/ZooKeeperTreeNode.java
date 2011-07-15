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
import org.apache.commons.lang.Validate;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Properties;

import static org.apache.commons.lang.StringUtils.isEmpty;

/**
 * Represents a node in the ZooKeeper tree.
 */
public class ZooKeeperTreeNode {
    private final int level;
    private final String parentPath;
    private final String path;
    private final String name;

    private final byte[] data;

    private final ImmutableMap<String, ZooKeeperTreeNode> children;

    private final boolean dataConsistent;
    private final boolean childListConsistent;
    private final boolean fullyConsistent;

    /**
     * Calculates the full path relative to the root of this nodes parent.
     *
     * @param parentPath the path to the parent of the current node (or null).
     * @param path       the path of the current node, this can be relative to the parent
     *                   or absolute if the parent is null.
     * @return the absolute path of the parent or null if this node is the root node.
     */
    public static String calculateParentPath(String parentPath, String path) {
        final boolean parentIsEmpty = isEmpty(parentPath);
        final boolean parentIsRoot = parentIsEmpty || "/".equals(parentPath);
        final boolean pathIsRoot = isEmpty(path) || "/".equals(path);

        // Eliminate the case where we are the root node.
        if (pathIsRoot) {
            // This check is a little forgiving.
            Validate.isTrue(parentIsRoot, "The parentPath must always be null for the root node.");
            return null;
        }

        Validate.isTrue(!path.endsWith("/"), "path cannot end with a /");

        // Eliminate absolute paths.
        if (parentIsEmpty) {
            Validate.isTrue(path.startsWith("/"), "path must start with a / if parentPath is not valid.");

            // Given the check on the previous line, there must be at least one slash.
            final int lastSlash = path.lastIndexOf('/');

            // Handle special case for root node.
            return (lastSlash == 0) ? "/" : path.substring(0, lastSlash);
        }

        // We are dealing with a relative path.
        Validate.isTrue(!path.contains("/"), "path cannot contain / if parentPath is valid.");

        // Eliminate the case where our parent is the root.
        if (parentIsRoot) {
            return "/";
        }

        Validate.isTrue(!parentPath.endsWith("/"), "parentPath cannot end with a / unless it is the root");

        return parentPath;
    }

    /**
     * Calculates the full path relative to the root.
     *
     * @param parentPath the path to the parent of the current node (or null).
     * @param path       the path of the current node, this can be relative to the parent
     *                   or absolute if the parent is null.
     * @return the absolute path.
     */
    public static String calculateFullPath(String parentPath, String path) {
        final boolean parentIsEmpty = isEmpty(parentPath);
        final boolean parentIsRoot = parentIsEmpty || "/".equals(parentPath);
        final boolean pathIsRoot = isEmpty(path) || "/".equals(path);

        // Eliminate the case where we are the root node.
        if (pathIsRoot) {
            // This check is a little forgiving.
            Validate.isTrue(parentIsRoot, "The parentPath must always be null for the root node.");
            return "/";
        }

        Validate.isTrue(!path.endsWith("/"), "path cannot end with a /");

        // Eliminate absolute paths.
        if (parentIsEmpty) {
            Validate.isTrue(path.startsWith("/"), "path must start with a / if parentPath is not valid.");

            return path;
        }

        // We are dealing with a relative path.
        Validate.isTrue(!path.contains("/"), "path cannot contain / if parentPath is valid.");

        // Eliminate the case where our parent is the root.
        if (parentIsRoot) {
            return "/" + path;
        }

        Validate.isTrue(!parentPath.endsWith("/"), "parentPath cannot end with a / unless it is the root");

        return parentPath + "/" + path;
    }

    /**
     * Calculates the name of the current node.
     *
     * @param parentPath the path to the parent of the current node (or null).
     * @param path       the path of the current node, this can be relative to the parent
     *                   or absolute if the parent is null.
     * @return the name of the node without and / unless it is the root node.
     */
    public static String calculateName(String parentPath, String path) {
        final boolean parentIsEmpty = isEmpty(parentPath);
        final boolean parentIsRoot = parentIsEmpty || "/".equals(parentPath);
        final boolean pathIsRoot = isEmpty(path) || "/".equals(path);

        // Eliminate the case where we are the root node.
        if (pathIsRoot) {
            // This check is a little forgiving.
            Validate.isTrue(parentIsRoot, "The parentPath must always be null for the root node.");
            return "/";
        }

        Validate.isTrue(!path.endsWith("/"), "path cannot end with a /");

        // Eliminate absolute paths.
        if (parentIsEmpty) {
            Validate.isTrue(path.startsWith("/"), "path must start with a / if parentPath is not valid.");

            return extractName(path);
        }

        // We are dealing with a relative path.
        Validate.isTrue(!path.contains("/"), "path cannot contain / if parentPath is valid.");

        // Eliminate the case where our parent is the root.
        if (parentIsRoot) {
            return extractName(path);
        }

        Validate.isTrue(!parentPath.endsWith("/"), "parentPath cannot end with a / unless it is the root");

        return extractName(path);
    }

    private static String extractName(String value) {
        final int lastSlash = value.lastIndexOf('/');

        return (lastSlash == -1) ? value : value.substring(lastSlash + 1);
    }

    /**
     * Creates a new ZooKeeperTreeNode with no children.
     * <p/>
     * All fields are final.
     *
     * @param level               the level of the node where each level in the tree is +1 over its parent.
     * @param path                the full path to this node.
     * @param data                the current value of the data stored in the node (may be null);
     * @param dataConsistent      true if the value (data) of this node is consistent with the value stored in ZK.
     * @param childListConsistent true if the list of child nodes is consistent with the list in ZK.
     * @param fullyConsistent     true if the entire sub-tree rooted at this node is consistent.
     */

    public ZooKeeperTreeNode(
            int level, String path, byte[] data,
            boolean dataConsistent, boolean childListConsistent, boolean fullyConsistent) {

        this(
                level, path,
                data, ImmutableMap.<String, ZooKeeperTreeNode>of(),
                dataConsistent, childListConsistent, fullyConsistent);
    }

    /**
     * Creates a new ZooKeeperTreeNode.
     * <p/>
     * All fields are final.
     *
     * @param level               the level of the node where each level in the tree is +1 over its parent.
     * @param path                the full path to this node.
     * @param data                the current value of the data stored in the node (may be null);
     * @param children            the list of child nodes (immutable).
     * @param dataConsistent      true if the value (data) of this node is consistent with the value stored in ZK.
     * @param childListConsistent true if the list of child nodes is consistent with the list in ZK.
     * @param fullyConsistent     true if the entire sub-tree rooted at this node is consistent.
     */
    public ZooKeeperTreeNode(
            int level, String path,
            byte[] data, ImmutableMap<String, ZooKeeperTreeNode> children,
            boolean dataConsistent, boolean childListConsistent, boolean fullyConsistent) {

        this(
                level,
                calculateParentPath(null, path),
                calculateFullPath(null, path),
                calculateName(null, path),
                data, children,
                dataConsistent, childListConsistent, fullyConsistent);
    }

    /**
     * Creates a new ZooKeeperTreeNode with separate path and name parameters.
     * <p/>
     * All fields are final.
     *
     * @param level               the level of the node where each level in the tree is +1 over its parent.
     * @param parentPath          the path of this nodes parent.
     * @param name                the name of this node (without any path).
     * @param data                the current value of the data stored in the node (may be null);
     * @param children            the list of child nodes (immutable).
     * @param dataConsistent      true if the value (data) of this node is consistent with the value stored in ZK.
     * @param childListConsistent true if the list of child nodes is consistent with the list in ZK.
     * @param fullyConsistent     true if the entire sub-tree rooted at this node is consistent.
     */
    public ZooKeeperTreeNode(
            int level, String parentPath, String name,
            byte[] data, ImmutableMap<String, ZooKeeperTreeNode> children,
            boolean dataConsistent, boolean childListConsistent, boolean fullyConsistent) {

        this(
                level,
                calculateParentPath(parentPath, name),
                calculateFullPath(parentPath, name),
                calculateName(parentPath, name),
                data, children,
                dataConsistent, childListConsistent, fullyConsistent);
    }

    public ZooKeeperTreeNode(ZooKeeperTreeNode prototype,
                             byte[] data, ImmutableMap<String, ZooKeeperTreeNode> children,
                             boolean dataConsistent, boolean childListConsistent, boolean fullyConsistent) {

        this(
                prototype.level, prototype.parentPath, prototype.path, prototype.name,
                data, children,
                dataConsistent, childListConsistent, fullyConsistent);
    }

    private ZooKeeperTreeNode(
            int level, String parentPath, String path, String name,
            byte[] data, ImmutableMap<String, ZooKeeperTreeNode> children,
            boolean dataConsistent, boolean childListConsistent, boolean fullyConsistent) {

        this.level = level;
        this.parentPath = parentPath;
        this.path = path;
        this.name = name;
        this.data = data;
        this.children = children;
        this.dataConsistent = dataConsistent;
        this.childListConsistent = childListConsistent;
        this.fullyConsistent = fullyConsistent;

        if (fullyConsistent) {
            Validate.isTrue(dataConsistent, "Data is not consistent");
            Validate.isTrue(childListConsistent, "Child list is not consistent");
        }
    }

    public int getLevel() {
        return level;
    }

    public String getParentPath() {
        return parentPath;
    }

    /**
     * Returns the full path of this node.
     *
     * @return parentPath + "/" + name
     */
    public String getPath() {
        return path;
    }

    public String getName() {
        return name;
    }

    /**
     * Returns the value of the node.
     * <p/>
     * Could be null if the node has no data and will be null if the tree manager is not tracking data at this level.
     *
     * @return the value of the data.
     */
    public byte[] getData() {
        return data;
    }

    public ZooKeeperTreeNode getChild(String childName) {
        return children.get(childName);
    }

    public ImmutableMap<String, ZooKeeperTreeNode> getChildren() {
        return children;
    }

    /**
     * Returns true if the data value is consistent with the value in ZooKeeper.
     * <p/>
     * This flag is cleared when getData() is executed.
     * It will always be true if the ZooKeeperTreeManager is not tracking the data at this tree level.
     *
     * @return true if the data is consistent.
     */
    public boolean isDataConsistent() {
        return dataConsistent;
    }

    /**
     * This flag is cleared when getChildren() is executed.
     *
     * @return true if the list of child nodes are consistent.
     */
    public boolean isChildListConsistent() {
        return childListConsistent;
    }

    /**
     * Returns is this entire sub-tree is consistent.
     *
     * @return true if previous two flags are true and all children are fullyConsistent.
     */

    public boolean isFullyConsistent() {
        return fullyConsistent;
    }

    @Override
    public String toString() {
        return "ZooKeeperTreeNode[" + "" + path + "]=" +
                "(" + dataConsistent + ", " + childListConsistent + ", " + fullyConsistent + ")";
    }

    /**
     * Assumes the data contains a serialized java property file.
     *
     * @return the data as a Properties object.
     */
    public Properties toProperties() {
        try {
            final Properties properties = new Properties();
            if (data != null) {
                properties.load(new ByteArrayInputStream(data));
            }
            return properties;
        } catch (IOException e) {
            // Wont happen.
            throw new IllegalStateException(e);
        }
    }
}
