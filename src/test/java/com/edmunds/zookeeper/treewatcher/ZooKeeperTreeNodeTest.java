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

import org.testng.annotations.Test;

import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test
public class ZooKeeperTreeNodeTest {
    @Test
    public void calculatePathsTestRoots() {
        assertEquals(ZooKeeperTreeNode.calculateParentPath(null, null), null);
        assertEquals(ZooKeeperTreeNode.calculateFullPath(null, null), "/");
        assertEquals(ZooKeeperTreeNode.calculateName(null, null), "/");

        assertEquals(ZooKeeperTreeNode.calculateParentPath(null, "/"), null);
        assertEquals(ZooKeeperTreeNode.calculateFullPath(null, "/"), "/");
        assertEquals(ZooKeeperTreeNode.calculateName(null, "/"), "/");

        assertEquals(ZooKeeperTreeNode.calculateParentPath("/", null), null);
        assertEquals(ZooKeeperTreeNode.calculateFullPath("/", null), "/");
        assertEquals(ZooKeeperTreeNode.calculateName("/", null), "/");

        assertEquals(ZooKeeperTreeNode.calculateParentPath("/", "/"), null);
        assertEquals(ZooKeeperTreeNode.calculateFullPath("/", "/"), "/");
        assertEquals(ZooKeeperTreeNode.calculateName("/", "/"), "/");
    }

    @Test
    public void calculatePathsTestAbsolutePaths() {
        assertEquals(ZooKeeperTreeNode.calculateParentPath(null, "/a"), "/");
        assertEquals(ZooKeeperTreeNode.calculateFullPath(null, "/a"), "/a");
        assertEquals(ZooKeeperTreeNode.calculateName(null, "/a"), "a");

        assertEquals(ZooKeeperTreeNode.calculateParentPath(null, "/a/b"), "/a");
        assertEquals(ZooKeeperTreeNode.calculateFullPath(null, "/a/b"), "/a/b");
        assertEquals(ZooKeeperTreeNode.calculateName(null, "/a/b"), "b");

        assertIllegalArgument("/", "/a");
    }

    @Test
    public void calculatePathsTestDoubleAbsolute() {
        assertIllegalArgument("/a", "/a");
    }

    @Test
    public void calculatePathsTestRootRelativePaths() {
        assertIllegalArgument(null, "a");

        assertEquals(ZooKeeperTreeNode.calculateParentPath("/", "a"), "/");
        assertEquals(ZooKeeperTreeNode.calculateFullPath("/", "a"), "/a");
        assertEquals(ZooKeeperTreeNode.calculateName("/", "a"), "a");
    }

    @Test
    public void calculatePathsTestSubFailure() {
        assertIllegalArgument("/a", null);
        assertIllegalArgument("/a", "/");
    }

    @Test
    public void calculatePathsTestSubValid() {
        assertEquals(ZooKeeperTreeNode.calculateParentPath("/a", "b"), "/a");
        assertEquals(ZooKeeperTreeNode.calculateFullPath("/a", "b"), "/a/b");
        assertEquals(ZooKeeperTreeNode.calculateName("/a", "b"), "b");
    }

    public void toPropertiesTestNull() {
        final ZooKeeperTreeNode treeNode = new ZooKeeperTreeNode(0, "/a", null, true, true, true);
        final Properties properties = treeNode.toProperties();
        assertNotNull(properties);
        assertEquals(properties.size(), 0);
    }

    public void toPropertiesTestValid() {
        final byte[] rawProperties = "hello=world".getBytes();

        final ZooKeeperTreeNode treeNode = new ZooKeeperTreeNode(0, "/a", rawProperties, true, true, true);
        final Properties properties = treeNode.toProperties();

        assertNotNull(properties);
        assertEquals(properties.size(), 1);
        assertEquals(properties.get("hello"), "world");
    }

    private void assertIllegalArgument(final String parentPath, final String path) {
        try {
            ZooKeeperTreeNode.calculateParentPath(parentPath, path);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        try {
            ZooKeeperTreeNode.calculateFullPath(parentPath, path);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }

        try {
            ZooKeeperTreeNode.calculateName(parentPath, path);
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
        }
    }
}
