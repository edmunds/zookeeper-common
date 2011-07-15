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

import com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type;
import org.testng.annotations.Test;

import java.util.List;

import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.DELETE;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.INSERT;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDeltaResult.Type.UPDATE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

@Test
public class ZooKeeperTreeDeltaResultTest {
    @Test
    public void addNodeTest() {
        final ZooKeeperTreeNode a = new ZooKeeperTreeNode(1, "/a", null, true, true, true);
        final ZooKeeperTreeNode b = new ZooKeeperTreeNode(1, "/b", null, true, true, true);
        final ZooKeeperTreeNode c = new ZooKeeperTreeNode(1, "/c", null, true, true, true);

        final ZooKeeperTreeDeltaResult result = new ZooKeeperTreeDeltaResult();

        result.addNode(INSERT, a);
        result.addNode(UPDATE, b);
        result.addNode(DELETE, c);

        final List<ZooKeeperTreeNode> insertedNodes = result.getInsertedNodes();
        final List<ZooKeeperTreeNode> updatedNodes = result.getUpdatedNodes();
        final List<ZooKeeperTreeNode> deletedNodes = result.getDeletedNodes();

        assertEquals(insertedNodes.size(), 1);
        assertEquals(updatedNodes.size(), 1);
        assertEquals(deletedNodes.size(), 1);

        assertSame(insertedNodes.get(0), a);
        assertSame(updatedNodes.get(0), b);
        assertSame(deletedNodes.get(0), c);
    }

    public void isEmptyTestTrue() {
        final ZooKeeperTreeDeltaResult result = new ZooKeeperTreeDeltaResult();

        assertTrue(result.isEmpty());
    }

    public void isEmptyTestInsert() {
        assertIsEmpty(INSERT);
    }

    public void isEmptyTestUpdate() {
        assertIsEmpty(UPDATE);
    }

    public void isEmptyTestDelete() {
        assertIsEmpty(DELETE);
    }

    private void assertIsEmpty(Type type) {
        final ZooKeeperTreeNode a = new ZooKeeperTreeNode(1, "/a", null, true, true, true);
        final ZooKeeperTreeDeltaResult result = new ZooKeeperTreeDeltaResult();

        result.addNode(type, a);
        assertFalse(result.isEmpty());
    }
}
