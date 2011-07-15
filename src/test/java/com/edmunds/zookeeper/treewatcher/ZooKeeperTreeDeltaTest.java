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
import org.easymock.Capture;
import org.easymock.classextension.IMocksControl;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDelta.Walk.LEAF_FIRST;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDelta.Walk.ROOT_FIRST;
import static com.edmunds.zookeeper.treewatcher.ZooKeeperTreeDelta.Walk.ROOT_ONLY;
import static org.easymock.classextension.EasyMock.capture;
import static org.easymock.classextension.EasyMock.createControl;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;

@Test
public class ZooKeeperTreeDeltaTest {
    private ZooKeeperTreeNode b;
    private ZooKeeperTreeNode c;
    private ZooKeeperTreeNode d;
    private ZooKeeperTreeNode e;
    private ZooKeeperTreeNode f;

    private ZooKeeperTreeState treeState1;
    private ZooKeeperTreeState treeState2;

    private IMocksControl control;
    private ZooKeeperTreeDeltaCallback callback;

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

        b = new ZooKeeperTreeNode(1, "/a/b", null, toMap(d, e), true, true, true);
        c = new ZooKeeperTreeNode(1, "/a/c", null, toMap(f), true, true, true);

        // Note that the b and c nodes are not connected to a.
        treeState1 = new ZooKeeperTreeState(0, "/a", null, true, true);
        treeState2 = new ZooKeeperTreeState(0, "/a", null, true, true);

        control = createControl();

        callback = control.createMock("callback", ZooKeeperTreeDeltaCallback.class);
    }

    @Test
    public void testNoChange() {
        treeState1.insertNode("/a", b);
        treeState1.insertNode("/a", c);

        treeState2.insertNode("/a", b);
        treeState2.insertNode("/a", c);

        control.replay();

        final ZooKeeperTreeDelta delta = new ZooKeeperTreeDelta(callback);
        delta.treeConsistent(treeState1.getRootNode(), treeState2.getRootNode());

        control.verify();
    }

    @Test
    public void testInsertC() {
        treeState1.insertNode("/a", b);

        treeState2.insertNode("/a", b);
        treeState2.insertNode("/a", c);

        final Capture<ZooKeeperTreeDeltaResult> cap = new Capture<ZooKeeperTreeDeltaResult>();
        callback.treeChanged(capture(cap));
        control.replay();

        final ZooKeeperTreeDelta delta = new ZooKeeperTreeDelta(callback);
        delta.treeConsistent(treeState1.getRootNode(), treeState2.getRootNode());

        control.verify();

        final ZooKeeperTreeDeltaResult result = cap.getValue();

        assertEquals(result.getInsertedNodes().size(), 1);
        assertEquals(result.getUpdatedNodes().size(), 0);
        assertEquals(result.getDeletedNodes().size(), 0);

        assertSame(result.getInsertedNodes().get(0), c);
    }

    @Test
    public void testInsertBLeafFirst() {
        treeState1.insertNode("/a", c);

        treeState2.insertNode("/a", b);
        treeState2.insertNode("/a", c);

        final Capture<ZooKeeperTreeDeltaResult> cap = new Capture<ZooKeeperTreeDeltaResult>();
        callback.treeChanged(capture(cap));
        control.replay();

        final ZooKeeperTreeDelta delta = new ZooKeeperTreeDelta(callback, ROOT_FIRST, LEAF_FIRST, ROOT_FIRST);
        delta.treeConsistent(treeState1.getRootNode(), treeState2.getRootNode());

        control.verify();

        final ZooKeeperTreeDeltaResult result = cap.getValue();

        final List<ZooKeeperTreeNode> inserts = result.getInsertedNodes();
        assertEquals(inserts.size(), 3);
        assertSame(inserts.get(0), d);
        assertSame(inserts.get(1), e);
        assertSame(inserts.get(2), b);

        assertEquals(result.getUpdatedNodes().size(), 0);
        assertEquals(result.getDeletedNodes().size(), 0);
    }

    @Test
    public void testUpdate() {
        treeState1.insertNode("/a", b);
        treeState1.insertNode("/a", c);
        treeState1.setData("/a/b/e", "hello".getBytes());

        treeState2 = new ZooKeeperTreeState(treeState1);

        treeState2.setData("/a/b/e", "world".getBytes());

        final Capture<ZooKeeperTreeDeltaResult> cap = new Capture<ZooKeeperTreeDeltaResult>();
        callback.treeChanged(capture(cap));
        control.replay();

        final ZooKeeperTreeDelta delta = new ZooKeeperTreeDelta(callback);
        delta.treeConsistent(treeState1.getRootNode(), treeState2.getRootNode());

        control.verify();

        final ZooKeeperTreeDeltaResult result = cap.getValue();

        assertEquals(result.getInsertedNodes().size(), 0);
        assertEquals(result.getUpdatedNodes().size(), 1);
        assertEquals(result.getDeletedNodes().size(), 0);
    }

    @Test
    public void testDeleteC() {
        treeState1.insertNode("/a", b);
        treeState1.insertNode("/a", c);

        treeState2.insertNode("/a", b);

        final Capture<ZooKeeperTreeDeltaResult> cap = new Capture<ZooKeeperTreeDeltaResult>();
        callback.treeChanged(capture(cap));
        control.replay();

        final ZooKeeperTreeDelta delta = new ZooKeeperTreeDelta(callback);
        delta.treeConsistent(treeState1.getRootNode(), treeState2.getRootNode());

        control.verify();

        final ZooKeeperTreeDeltaResult result = cap.getValue();

        assertEquals(result.getInsertedNodes().size(), 0);
        assertEquals(result.getUpdatedNodes().size(), 0);
        assertEquals(result.getDeletedNodes().size(), 1);

        assertSame(result.getDeletedNodes().get(0), c);
    }

    @Test
    public void testDeleteBRootFirst() {
        treeState1.insertNode("/a", b);
        treeState1.insertNode("/a", c);

        treeState2.insertNode("/a", c);

        final Capture<ZooKeeperTreeDeltaResult> cap = new Capture<ZooKeeperTreeDeltaResult>();
        callback.treeChanged(capture(cap));
        control.replay();

        final ZooKeeperTreeDelta delta = new ZooKeeperTreeDelta(callback, ROOT_FIRST, ROOT_ONLY, ROOT_FIRST);
        delta.treeConsistent(treeState1.getRootNode(), treeState2.getRootNode());

        control.verify();

        final ZooKeeperTreeDeltaResult result = cap.getValue();

        assertEquals(result.getInsertedNodes().size(), 0);
        assertEquals(result.getUpdatedNodes().size(), 0);

        final List<ZooKeeperTreeNode> deletes = result.getDeletedNodes();
        assertEquals(deletes.size(), 3);
        assertSame(deletes.get(0), b);
        assertSame(deletes.get(1), d);
        assertSame(deletes.get(2), e);
    }

    @Test
    public void testDeleteTwoLeafFirst() {
        treeState1.insertNode("/a", b);
        treeState1.insertNode("/a", c);

        treeState2 = new ZooKeeperTreeState(treeState1);
        treeState2.deleteNode("/a/c/f");
        treeState2.deleteNode("/a/b");

        final Capture<ZooKeeperTreeDeltaResult> cap = new Capture<ZooKeeperTreeDeltaResult>();
        callback.treeChanged(capture(cap));
        control.replay();

        final ZooKeeperTreeDelta delta = new ZooKeeperTreeDelta(callback, LEAF_FIRST, LEAF_FIRST, LEAF_FIRST);
        delta.treeConsistent(treeState1.getRootNode(), treeState2.getRootNode());

        control.verify();

        final ZooKeeperTreeDeltaResult result = cap.getValue();

        assertEquals(result.getInsertedNodes().size(), 0);
        assertEquals(result.getUpdatedNodes().size(), 0);

        final List<ZooKeeperTreeNode> deleted = result.getDeletedNodes();
        assertEquals(deleted.size(), 4);
        assertEquals(deleted.get(0), f);
        assertEquals(deleted.get(1), d);
        assertEquals(deleted.get(2), e);
        assertEquals(deleted.get(3), b);
    }

    private ImmutableMap<String, ZooKeeperTreeNode> toMap(ZooKeeperTreeNode a) {
        return ImmutableMap.of(a.getName(), a);
    }

    private ImmutableMap<String, ZooKeeperTreeNode> toMap(ZooKeeperTreeNode a, ZooKeeperTreeNode b) {
        return ImmutableMap.of(a.getName(), a, b.getName(), b);
    }
}
