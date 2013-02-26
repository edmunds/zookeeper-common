package com.edmunds.zookeeper.election;

import com.edmunds.zookeeper.connection.ZooKeeperConnection;
import org.easymock.classextension.EasyMock;
import org.easymock.classextension.IMocksControl;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

@Test
public class ZooKeeperElectionTest {

    private IMocksControl control;
    private ZooKeeperConnection connection;

    @BeforeMethod
    public void setup() {

        this.control = EasyMock.createControl();
        this.connection = control.createMock("connection", ZooKeeperConnection.class);
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

    @AfterMethod
    public void tearDown() {
        this.control = null;
        this.connection = null;
    }
}
