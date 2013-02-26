package com.edmunds.zookeeper.election;

import com.google.common.collect.Lists;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;

import static org.testng.Assert.assertEquals;

public class ZooKeeperSequentialValueComparatorTest {

    @Test
    public void sortTest() {
        List<String> list = Lists.newArrayList();

        list.add("alpha-1234-abcd-12ab-00005");
        list.add("alpha-9999-abcd-12ab-00002");
        list.add("gamma-8888-abcd-12ab-00003");
        list.add("alpha-7777-abcd-12ab-00004");
        list.add("gamma-1234-abcd-12ab-00001");

        Collections.sort(list, new ZooKeeperSequentialValueComparator());

        assertEquals(list.get(0), "alpha-9999-abcd-12ab-00002");
        assertEquals(list.get(1), "alpha-7777-abcd-12ab-00004");
        assertEquals(list.get(2), "alpha-1234-abcd-12ab-00005");
        assertEquals(list.get(3), "gamma-1234-abcd-12ab-00001");
        assertEquals(list.get(4), "gamma-8888-abcd-12ab-00003");
    }
}
