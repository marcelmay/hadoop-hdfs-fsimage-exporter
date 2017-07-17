package de.m3y.prometheus.exporter.fsimage;

import de.m3y.hadoop.hdfs.hfsa.util.SizeBucket;

/**
 * Models the file size buckets.
 * <p>
 * <ul>
 * <li>0 B - indicates too small file for HDFS, and might be suspicious</li>
 * <li>1 MiB - indicates too small file for HDFS</li>
 * <li>32 MiB - still small</li>
 * <li>64 MiB - still small</li>
 * <li>128 MiB - default dfs block size</li>
 * <li>1 GiB - large</li>
 * <li>10 GiB - very large</li>
 * <li>10+ GiB - really large</li>
 * </ul>
 */
class FixedBucketModel implements SizeBucket.BucketModel {
    private static final int MAX_BUCKET = 7;
    private static final long SIZE_1_MIB = 1024L * 1024L;
    private static final long SIZE_1_GIB = 1024L * SIZE_1_MIB;

    @Override
    public int computeBucket(long l) {
        if (l == 0L /* 0 B */) {
            return 0;
        } else if (l <= SIZE_1_MIB /* 1 MiB */) {
            return 1;
        } else if (l <= 32L * SIZE_1_MIB /* 32 MiB */) {
            return 2;
        } else if (l <= 64L * SIZE_1_MIB /* 64 MiB */) {
            return 3;
        } else if (l <= 128L * SIZE_1_MIB /* 128 MiB */) {
            return 4;
        } else if (l <= SIZE_1_GIB /* 1 GiB */) {
            return 5;
        } else if (l <= 10L * SIZE_1_GIB /* 10 GiB */) {
            return 6;
        }
        return MAX_BUCKET; // > 10 GiB
    }

    private static final long[] UPPER_BOUNDARIES = new long[]{
            0L /* 0 B */,
            SIZE_1_MIB /* 1 MiB */,
            32L * SIZE_1_MIB /* 32 MiB */,
            64L * SIZE_1_MIB /* 64 MiB */,
            128L * SIZE_1_MIB /* 128 MiB */,
            SIZE_1_GIB /* 1 GiB */,
            10L * SIZE_1_GIB /* 10 GiB */,
            Long.MAX_VALUE /* > 10 GiB */
    };

    @Override
    public long[] computeBucketUpperBorders(int i) {
        return UPPER_BOUNDARIES;
    }

    @Override
    public int getInitialNumberOfBuckets() {
        return MAX_BUCKET + 1;
    }
}
