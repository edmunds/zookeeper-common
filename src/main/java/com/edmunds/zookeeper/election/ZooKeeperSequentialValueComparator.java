package com.edmunds.zookeeper.election;

import java.util.Comparator;

/**
 * Compares to Zookeeper nodes based on their prefix and sequential values.
 */
public class ZooKeeperSequentialValueComparator implements Comparator<String> {
    @Override
    public int compare(String left, String right) {
        return new SequentialValue(left).compareTo(new SequentialValue(right));
    }

    private static class SequentialValue implements Comparable<SequentialValue> {
        private final String start;
        private final String end;

        SequentialValue(String path) {
            final String[] split = path.split("-");

            start = split[0];
            end = split[split.length - 1];
        }

        @Override
        public int compareTo(SequentialValue other) {
            final int result = start.compareTo(other.start);

            if (result != 0) {
                return result;
            }

            return end.compareTo(other.end);
        }
    }
}
