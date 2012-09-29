package com.edmunds.zookeeper.election;

import com.edmunds.zookeeper.connection.ZooKeeperConnection;
import com.google.common.collect.Lists;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.easymock.Capture;
import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;

import static org.easymock.EasyMock.endsWith;
import static org.easymock.classextension.EasyMock.capture;
import static org.easymock.classextension.EasyMock.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test
public class ZooKeeperElectionTest {

    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private static final Stat DUMMY_STAT = new Stat();
    private static final List<String> ONE_CHILD = Lists.newArrayList("node-002");
    private static final List<String> TWO_CHILDREN = Lists.newArrayList("node-002", "node-001");

    private Capture<AsyncCallback.StringCallback> callbackCreatePrimary;
    private Capture<ZooKeeperElectionContext> zooKeeperElectionContextCapture;
    private Capture<AsyncCallback.Children2Callback> callbackListChildren;
    private Capture<Watcher> callbackGetDataWatcher;
    private Capture<AsyncCallback.DataCallback> callbackGetData;

    private IMocksControl control;
    private ZooKeeperConnection connection;
    private ZooKeeperElectionListener listener;

    @BeforeMethod
    public void setup() {
        this.callbackCreatePrimary = new Capture<AsyncCallback.StringCallback>();
        this.zooKeeperElectionContextCapture = new Capture<ZooKeeperElectionContext>();
        this.callbackListChildren = new Capture<AsyncCallback.Children2Callback>();
        this.callbackGetDataWatcher = new Capture<Watcher>();
        this.callbackGetData = new Capture<AsyncCallback.DataCallback>();

        this.control = EasyMock.createControl();
        this.connection = control.createMock("connection", ZooKeeperConnection.class);
        this.listener = control.createMock("listener", ZooKeeperElectionListener.class);
    }

    @Test
    public void testBadRootPathMissingSlash() {
        control.replay();

        try {
            new ZooKeeperElection(connection, "election");
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals(e.getClass(), RuntimeException.class);
            assertEquals(e.getMessage(), "Invalid election root path: election");
        }

        control.verify();
    }

    @Test
    public void testBadRootPathEndSlash() {
        control.replay();

        try {
            new ZooKeeperElection(connection, "/election/");
            fail("Expected RuntimeException");
        } catch (RuntimeException e) {
            assertEquals(e.getClass(), RuntimeException.class);
            assertEquals(e.getMessage(), "Invalid election root path: /election/");
        }

        control.verify();
    }

    @Test
    public void testWithdraw() {
        connection.createEphemeralSequential(
                eq("/election/node-"), EasyMock.<byte[]>anyObject(),
                capture(callbackCreatePrimary), capture(zooKeeperElectionContextCapture));

        connection.getChildren(
                eq("/election"), EasyMock.<Watcher>isNull(),
                capture(callbackListChildren), EasyMock.<Object>anyObject());

        connection.getData(
                eq("/election/node-001"), capture(callbackGetDataWatcher),
                capture(callbackGetData), EasyMock.<Object>anyObject());

        connection.getData(
                eq("/election/node-002"), capture(callbackGetDataWatcher),
                capture(callbackGetData), EasyMock.<Object>anyObject());

        connection.delete("/election/node-002", -1, null, null);

        // REPLAY
        control.replay();

        final ZooKeeperElection elect = new ZooKeeperElection(connection, "/election");
        elect.enroll();

        final ZooKeeperElectionContext ctx = zooKeeperElectionContextCapture.getValue();

        callbackCreatePrimary.getValue().processResult(0, "/election/node-", ctx, "/election/node-002");
        callbackListChildren.getValue().processResult(0, "/election", ctx, TWO_CHILDREN, DUMMY_STAT);

        callbackGetData.getValue().processResult(
                0, "/election/node-001", ctx, EMPTY_BYTE_ARRAY, DUMMY_STAT);

        elect.withdraw();

        // After withdrawing force a master election.

        callbackGetDataWatcher.getValue().process(
                new WatchedEvent(EventType.NodeDeleted, KeeperState.SyncConnected, "/election/node-001"));

        // VERIFY
        control.verify();

        assertEquals(elect.getRootPath(), "/election");
    }

    @Test
    public void testLostConnection() {
        connection.createEphemeralSequential(
                eq("/election/node-"), EasyMock.<byte[]>anyObject(),
                capture(callbackCreatePrimary), capture(zooKeeperElectionContextCapture));

        connection.getChildren(
                eq("/election"), EasyMock.<Watcher>isNull(),
                capture(callbackListChildren), EasyMock.<Object>anyObject());

        connection.getData(
                eq("/election/node-001"), capture(callbackGetDataWatcher),
                capture(callbackGetData), EasyMock.<Object>anyObject());

        connection.getData(
                eq("/election/node-002"), capture(callbackGetDataWatcher),
                capture(callbackGetData), EasyMock.<Object>anyObject());

        connection.delete("/election/node-002", -1, null, null);

        // REPLAY
        control.replay();

        final ZooKeeperElection elect = new ZooKeeperElection(connection, "/election");
        elect.enroll();

        final ZooKeeperElectionContext ctx = zooKeeperElectionContextCapture.getValue();

        callbackCreatePrimary.getValue().processResult(0, "/election/node-", ctx, "/election/node-002");
        callbackListChildren.getValue().processResult(0, "/election", ctx, TWO_CHILDREN, DUMMY_STAT);

        callbackGetData.getValue().processResult(
                0, "/election/node-001", ctx, EMPTY_BYTE_ARRAY, DUMMY_STAT);

        callbackGetDataWatcher.getValue().process(
                new WatchedEvent(null, KeeperState.Disconnected, null));

        // VERIFY
        control.verify();
    }

    @AfterMethod
    public void tearDown() {
        this.callbackCreatePrimary = null;
        this.callbackListChildren = null;
        this.callbackGetDataWatcher = null;
        this.callbackGetData = null;

        this.control = null;
        this.connection = null;
        this.listener = null;
    }
}
