package org.apache.spark.shuffle.daos;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JavaTest {

    static long[] generateArray() {
        int numPartitions = 2;
        Map<Integer, Integer> partitionBufMap = new HashMap<>();
        partitionBufMap.put(0, 0);
        partitionBufMap.put(1, 1);
        long[] lens = new long[numPartitions];
        partitionBufMap.values().stream().sorted().forEach(b -> lens[b] = b + 6);
        for (long l : lens) {
            System.out.println(l);
        }
        return lens;
    }

    @Test
    public void testStreamApi() throws Exception {
        generateArray();
    }
}
