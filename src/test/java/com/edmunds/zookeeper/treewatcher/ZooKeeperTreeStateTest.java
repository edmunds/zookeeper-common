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
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test
public class ZooKeeperTreeStateTest {
    private ZooKeeperTreeNode d;
    private ZooKeeperTreeNode e;
    private ZooKeeperTreeNode f;

    private ZooKeeperTreeState treeState;

    /* Builds the following node structure:
        a
        +-b
        | +-d
        | +-e
        +-c
          +-f
   */
    @BeforeMethod
    public void setup() {

        d = new ZooKeeperTreeNode(2, "/a/b/d", null, true, true, true);
        e = new ZooKeeperTreeNode(2, "/a/b/e", null, true, true, true);
        f = new ZooKeeperTreeNode(2, "/a/c/f", null, true, true, true);

        final ZooKeeperTreeNode b = new ZooKeeperTreeNode(1, "/a/b", null, toMap(d, e), true, true, true);
        final ZooKeeperTreeNode c = new ZooKeeperTreeNode(1, "/a/c", null, toMap(f), true, true, true);

        // Note that the b and c nodes are not connected to a.

        treeState = new ZooKeeperTreeState(0, "/a", null, true, true);

        treeState.insertNode("/a", b);
        treeState.insertNode("/a", c);
    }

    @Test
    public void testCopyConstructor() {
        ZooKeeperTreeState oldTreeState = treeState;
        treeState = new ZooKeeperTreeState(oldTreeState);
        validate();

        assertEquals(treeState.getRootNode(), oldTreeState.getRootNode());
    }

    @Test
    public void testSimpleTree() {
        validate();
        assertEquals(treeState.getNode("/a/c/f"), f);

        assertWalk(treeState, treeState.getNode("/a"), treeState.getNode("/a/b"), d, e, treeState.getNode("/a/c"), f);
        assertWalk(treeState, "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/c", "/a/c/f");
    }

    @Test
    public void testDeleteTree() {
        treeState.deleteNode("/a/b");
        validate();

        assertWalk(treeState, "/a", "/a/c", "/a/c/f");
    }

    @Test
    public void testDeleteNode() {
        treeState.deleteNode("/a/b/d");
        validate();

        assertWalk(treeState, "/a", "/a/b", "/a/b/e", "/a/c", "/a/c/f");
    }

    @Test
    public void testDuplicateInsert() {
        try {
            treeState.insertNode("/a", new ZooKeeperTreeNode(1, "/a/b", null, null, true, true, true));
            fail("Expected IllegalStateException");
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Cannot overwrite existing node: /a/b");
        }
    }

    @Test
    public void testSimpleInsertAndDelete() {
        final ZooKeeperTreeNode x = new ZooKeeperTreeNode(3, "/a/b/e/x", null, false, true, false);

        treeState.insertNode("/a/b/e", x);
        //validate();
        assertWalk(treeState, "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/b/e/x", "/a/c", "/a/c/f");

        treeState.deleteNode("/a/b/e/x");
        validate();
    }

    @Test
    public void testInsertInconsistentNode() {
        final ZooKeeperTreeNode x = new ZooKeeperTreeNode(3, "/a/b/e/x", null, false, true, false);

        treeState.insertNode("/a/b/e", x);
        validate();
        assertWalk(treeState, "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/b/e/x", "/a/c", "/a/c/f");

        assertFalse(treeState.getNode("/a").isFullyConsistent());
        assertFalse(treeState.getNode("/a/b").isFullyConsistent());
        assertTrue(treeState.getNode("/a/b/d").isFullyConsistent());
        assertFalse(treeState.getNode("/a/b/e").isFullyConsistent());
        assertFalse(treeState.getNode("/a/b/e/x").isFullyConsistent());
        assertTrue(treeState.getNode("/a/c").isFullyConsistent());
        assertTrue(treeState.getNode("/a/c/f").isFullyConsistent());

        final ZooKeeperTreeNode y = new ZooKeeperTreeNode(3, "/a/b/d/y", null, true, true, true);
        treeState.insertNode("/a/b/d", y);
        validate();

        assertWalk(treeState, "/a", "/a/b", "/a/b/d", "/a/b/d/y", "/a/b/e", "/a/b/e/x", "/a/c", "/a/c/f");

        assertFalse(treeState.getNode("/a").isFullyConsistent());
        assertFalse(treeState.getNode("/a/b").isFullyConsistent());
        assertTrue(treeState.getNode("/a/b/d").isFullyConsistent());
        assertTrue(treeState.getNode("/a/b/d/y").isFullyConsistent());
        assertFalse(treeState.getNode("/a/b/e").isFullyConsistent());
        assertFalse(treeState.getNode("/a/b/e/x").isFullyConsistent());
        assertTrue(treeState.getNode("/a/c").isFullyConsistent());
        assertTrue(treeState.getNode("/a/c/f").isFullyConsistent());
    }

    @Test
    public void testSetData() {
        final byte[] eBytes = "E Node".getBytes();
        final byte[] xBytes = "X Node".getBytes();
        final ZooKeeperTreeNode x = new ZooKeeperTreeNode(3, "/a/b/e/x", null, false, true, false);

        treeState.setData("/a/b/e", eBytes);
        treeState.insertNode("/a/b/e", x);
        treeState.setData("/a/b/e/x", xBytes);

        validate();
        assertWalk(treeState, "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/b/e/x", "/a/c", "/a/c/f");

        assertTrue(treeState.getNode("/a").isFullyConsistent());
        assertTrue(treeState.getNode("/a/b").isFullyConsistent());
        assertTrue(treeState.getNode("/a/b/d").isFullyConsistent());
        assertTrue(treeState.getNode("/a/b/e").isFullyConsistent());
        assertTrue(treeState.getNode("/a/b/e/x").isFullyConsistent());
        assertTrue(treeState.getNode("/a/c").isFullyConsistent());
        assertTrue(treeState.getNode("/a/c/f").isFullyConsistent());

        assertEquals(treeState.getNode("/a/b/e").getData(), eBytes);
        assertEquals(treeState.getNode("/a/b/e/x").getData(), xBytes);
    }

    @Test
    public void setFlagsTestInvalidate() {

        treeState.setFlags("/a/b", false, true);
        validate();

        final ZooKeeperTreeNode a = treeState.getNode("/a");
        assertTrue(a.isDataConsistent());
        assertTrue(a.isChildListConsistent());
        assertFalse(a.isFullyConsistent());

        final ZooKeeperTreeNode b = treeState.getNode("/a/b");
        assertFalse(b.isDataConsistent());
        assertTrue(b.isChildListConsistent());
        assertFalse(b.isFullyConsistent());
    }

    @Test
    public void setChildrenTestInsert() {
        treeState.setChildren("/a/b", Arrays.asList("d", "e", "z"));
        validate();

        assertWalk(treeState, "/a", "/a/b", "/a/b/d", "/a/b/e", "/a/b/z", "/a/c", "/a/c/f");
        assertEquals(treeState.getRootNode().isFullyConsistent(), false);
    }

    @Test
    public void setChildrenTestDelete() {
        treeState.setChildren("/a/b", Collections.singleton("e"));
        validate();

        assertWalk(treeState, "/a", "/a/b", "/a/b/e", "/a/c", "/a/c/f");
    }

    private void assertWalk(ZooKeeperTreeState treeState, String... paths) {
        final List<ZooKeeperTreeNode> nodes = walkTree(treeState.getRootNode());

        assertEquals(nodes.size(), paths.length);

        for (int i = 0; i < paths.length; i++) {
            final String path = paths[i];
            final ZooKeeperTreeNode expectedNode = treeState.getNode(path);
            assertNotNull(expectedNode, "Node was null: " + path);
            assertEquals(nodes.get(i), expectedNode, "Paths do not match: " + path);
        }
    }

    private void assertWalk(ZooKeeperTreeState treeState, ZooKeeperTreeNode... expectedNodes) {
        final List<ZooKeeperTreeNode> nodes = walkTree(treeState.getRootNode());

        assertEquals(nodes.size(), expectedNodes.length);

        for (int i = 0; i < expectedNodes.length; i++) {
            final ZooKeeperTreeNode expectedNode = expectedNodes[i];
            assertNotNull(expectedNode, "Node was null: " + i);
            assertEquals(nodes.get(i), expectedNode, "Paths do not match: " + expectedNode.getPath());
        }
    }

    private List<ZooKeeperTreeNode> walkTree(ZooKeeperTreeNode node) {
        List<ZooKeeperTreeNode> nodes = Lists.newArrayList();
        walkTree(node, nodes);
        return nodes;
    }

    private void walkTree(ZooKeeperTreeNode node, List<ZooKeeperTreeNode> nodes) {
        nodes.add(node);
        for (ZooKeeperTreeNode child : node.getChildren().values()) {
            walkTree(child, nodes);
        }
    }

    private ImmutableMap<String, ZooKeeperTreeNode> toMap(ZooKeeperTreeNode a) {
        return ImmutableMap.of(a.getName(), a);
    }

    private ImmutableMap<String, ZooKeeperTreeNode> toMap(ZooKeeperTreeNode a, ZooKeeperTreeNode b) {
        return ImmutableMap.of(a.getName(), a, b.getName(), b);
    }

    private void validate() {
        final int count = validate(treeState.getRootNode(), 0);

        // The root doesn't have a parent.
        assertEquals(treeState.getParentMappingSize(), count - 1);
        assertEquals(treeState.getPathMappingSize(), count);
    }

    private int validate(ZooKeeperTreeNode parent, int expectedLevel) {
        int count = 1;

        if (parent.getLevel() != expectedLevel) {
            throw new RuntimeException("Level is incorrect");
        }

        final int childLevel = expectedLevel + 1;
        for (Map.Entry<String, ZooKeeperTreeNode> entry : parent.getChildren().entrySet()) {
            final String name = entry.getKey();
            final ZooKeeperTreeNode node = entry.getValue();

            if (name == null) {
                throw new RuntimeException("name is null");
            }

            if (!name.equals(node.getName())) {
                throw new RuntimeException("Node name is incorrect: " +
                        node.getPath() + " [" + name + "] != [" + node.getName() + "]");
            }

            final ZooKeeperTreeNode testParent = treeState.getParent(node);

            if (testParent == null) {
                throw new RuntimeException("Parent is null");
            }

            if (testParent != parent) {
                throw new RuntimeException("Parent is not correct: [" + testParent + "] [" + parent + "]");
            }

            if (!name.equals(node.getName())) {
                throw new RuntimeException("name does not match");
            }

            final ZooKeeperTreeNode testNode = treeState.getNode(node.getPath());

            if (testNode == null) {
                throw new RuntimeException("testNode is null");
            }

            if (testNode != node) {
                throw new RuntimeException("testNode is incorrect");
            }

            count += validate(node, childLevel);
        }

        return count;
    }
}
