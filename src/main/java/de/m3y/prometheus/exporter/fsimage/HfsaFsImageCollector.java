package de.m3y.prometheus.exporter.fsimage;

import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HfsaFsImageCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(HfsaFsImageCollector.class);

    static final String METRIC_PREFIX = "fsimage_";

    private static final Counter METRIC_SCRAPE_REQUESTS = Counter.build()
            .name(METRIC_PREFIX + "scrape_requests_total")
            .help("Exporter requests made").register();
    private static final Counter METRIC_SCRAPE_ERROR = Counter.build()
            .name(METRIC_PREFIX + "scrape_errors_total")
            .help("Counts failed scrapes.").register();

    private static final Gauge METRIC_SCRAPE_DURATION = Gauge.build()
            .name(METRIC_PREFIX + "scrape_duration_seconds")
            .help("Scrape duration").register();
    private static final Gauge METRIC_EXPORTER_HEAP = Gauge.build()
            .name(METRIC_PREFIX + "jvm_heap_bytes")
            .labelNames("heap_type")
            .help("Exporter JVM heap").register();

    final Config config;
    final MemoryMXBean memoryMXBean;
    final FsImageWatcher fsImageWatcher;

    static class UserStats {
        final String userName;
        long sumBlocks;
        long sumDirectories;
        long sumSymLinks;

        UserStats(String userName) {
            this.userName = userName;
        }
    }

    static class GroupStats {
        final String groupName;
        long sumBlocks;
        long sumDirectories;
        long sumSymLinks;

        GroupStats(String groupName) {
            this.groupName = groupName;
        }
    }

    private static final long SIZE_1_MIB = 1024L * 1024L;
    private static final long SIZE_1_GIB = 1024L * SIZE_1_MIB;
    private static final long[] BUCKET_UPPER_BOUNDARIES = new long[]{
            0L /* 0 B */,
            SIZE_1_MIB /* 1 MiB */,
            32L * SIZE_1_MIB /* 32 MiB */,
            64L * SIZE_1_MIB /* 64 MiB */,
            128L * SIZE_1_MIB /* 128 MiB */,
            SIZE_1_GIB /* 1 GiB */,
            10L * SIZE_1_GIB /* 10 GiB */
    };

    private static final String METRIC_POSTFIX_DIRS = "dirs";
    private static final String METRIC_POSTFIX_BLOCKS = "blocks";
    private static final String METRIC_POSTFIX_LINKS = "links";
    private static final String FSIZE = "fsize";
    private static final String LABEL_USER_NAME = "user_name";

    // Overall
    static final Gauge METRIC_SUM_DIRS = Gauge.build()
            .name(METRIC_PREFIX + METRIC_POSTFIX_DIRS)
            .help("Number of directories.").register();
    static final Gauge METRIC_SUM_LINKS = Gauge.build()
            .name(METRIC_PREFIX + METRIC_POSTFIX_LINKS)
            .help("Number of sym links.").register();
    static final Gauge METRIC_SUM_BLOCKS = Gauge.build()
            .name(METRIC_PREFIX + METRIC_POSTFIX_BLOCKS)
            .help("Number of blocks.").register();
    static final Histogram METRIC_FILE_SIZE_BUCKETS = Histogram.build()
            .name(METRIC_PREFIX + FSIZE)
            .buckets(Arrays.stream(BUCKET_UPPER_BOUNDARIES).asDoubleStream().toArray())
            .help("Overall file size distribution")
            .register();


    // By user
    static final String METRIC_PREFIX_USER = METRIC_PREFIX + "user_";
    static final Gauge METRIC_USER_SUM_DIRS = Gauge.build()
            .name(METRIC_PREFIX_USER + METRIC_POSTFIX_DIRS)
            .labelNames(LABEL_USER_NAME)
            .help("Number of directories.").register();
    static final Gauge METRIC_USER_SUM_LINKS = Gauge.build()
            .name(METRIC_PREFIX_USER + METRIC_POSTFIX_LINKS)
            .labelNames(LABEL_USER_NAME)
            .help("Number of sym links.").register();
    static final Gauge METRIC_USER_SUM_BLOCKS = Gauge.build()
            .name(METRIC_PREFIX_USER + METRIC_POSTFIX_BLOCKS)
            .labelNames(LABEL_USER_NAME)
            .help("Number of blocks.").register();
    static final Histogram METRIC_USER_FILE_SIZE_BUCKETS = Histogram.build()
            .name(METRIC_PREFIX_USER + FSIZE)
            .labelNames(LABEL_USER_NAME)
            .buckets(Arrays.stream(BUCKET_UPPER_BOUNDARIES).asDoubleStream().toArray())
            .help("Per user file size distribution")
            .register();

    // By group
    static final String METRIC_PREFIX_GROUP = METRIC_PREFIX + "group_";
    static final Gauge METRIC_GROUP_SUM_DIRS = Gauge.build()
            .name(METRIC_PREFIX_GROUP + METRIC_POSTFIX_DIRS)
            .labelNames("group_name")
            .help("Number of directories.").register();
    static final Gauge METRIC_GROUP_SUM_LINKS = Gauge.build()
            .name(METRIC_PREFIX_GROUP + METRIC_POSTFIX_LINKS)
            .labelNames("group_name")
            .help("Number of sym links.").register();
    static final Gauge METRIC_GROUP_SUM_BLOCKS = Gauge.build()
            .name(METRIC_PREFIX_GROUP + METRIC_POSTFIX_BLOCKS)
            .labelNames("group_name")
            .help("Number of blocks.").register();
    static final Histogram METRIC_GROUP_FILE_SIZE_BUCKETS = Histogram.build()
            .name(METRIC_PREFIX_GROUP + FSIZE)
            .labelNames("group_name")
            .buckets(Arrays.stream(BUCKET_UPPER_BOUNDARIES).asDoubleStream().toArray())
            .help("Per group file size distribution.")
            .register();

    static class OverallStats {
        long sumDirectories;
        long sumBlocks;
        long sumSymLinks;
    }

    static class Report {
        final Map<String, GroupStats> groupStats;
        final Map<String, UserStats> userStats;
        final OverallStats overallStats;

        Report() {
            groupStats = Collections.synchronizedMap(new HashMap<>());
            userStats = Collections.synchronizedMap(new HashMap<>());
            overallStats = new OverallStats();
        }
    }

    HfsaFsImageCollector(Config config) {
        this.config = config;
        memoryMXBean = ManagementFactory.getMemoryMXBean();

        final String path = config.getFsImagePath();
        if (null == path || path.isEmpty()) {
            throw new IllegalArgumentException("Must provide the directory location to the FSImage snapshots");
        }

        File fsImageDir = new File(path);
        if (!fsImageDir.exists()) {
            throw new IllegalArgumentException(fsImageDir.getAbsolutePath() + " does not exist");
        }
        fsImageWatcher = new FsImageWatcher(fsImageDir, config.isSkipPreviouslyParsed());
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleWithFixedDelay(fsImageWatcher, 60, 60, TimeUnit.SECONDS);
    }

    private void scrape() throws IOException {
        METRIC_GROUP_FILE_SIZE_BUCKETS.clear();
        METRIC_USER_FILE_SIZE_BUCKETS.clear();
        METRIC_FILE_SIZE_BUCKETS.clear();

        final FSImageLoader loader = fsImageWatcher.getFSImageLoader();

        LOGGER.info("Visiting ...");
        long start = System.currentTimeMillis();
        Report report = computeStats(loader);
        LOGGER.info("Visiting finished [{}ms].", System.currentTimeMillis() - start);

        // Overall stats
        OverallStats overallStats = report.overallStats;
        METRIC_SUM_DIRS.set(overallStats.sumDirectories);
        METRIC_SUM_LINKS.set(overallStats.sumSymLinks);
        METRIC_SUM_BLOCKS.set(overallStats.sumBlocks);
        METRIC_SUM_BLOCKS.inc();

        // User stats
        for (UserStats userStat : report.userStats.values()) {
            String[] labelValues = new String[]{userStat.userName};
            METRIC_USER_SUM_DIRS.labels(labelValues).set(userStat.sumDirectories);
            METRIC_USER_SUM_LINKS.labels(labelValues).set(userStat.sumSymLinks);
            METRIC_USER_SUM_BLOCKS.labels(labelValues).set(userStat.sumBlocks);
        }

        // Group stats
        for (GroupStats groupStat : report.groupStats.values()) {
            String[] labelValues = new String[]{groupStat.groupName};
            METRIC_GROUP_SUM_DIRS.labels(labelValues).set(groupStat.sumDirectories);
            METRIC_GROUP_SUM_LINKS.labels(labelValues).set(groupStat.sumSymLinks);
            METRIC_GROUP_SUM_BLOCKS.labels(labelValues).set(groupStat.sumBlocks);
        }
    }

    private Report computeStats(FSImageLoader loader) throws IOException {
        Report report = new Report();
        final OverallStats overallStats = report.overallStats;
        loader.visitParallel(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                PermissionStatus p = loader.getPermissionStatus(f.getPermission());

                final long fileSize = FSImageLoader.getFileSize(f);
                final long fileBlocks = f.getBlocksCount();
                synchronized (overallStats) {
                    overallStats.sumBlocks += fileBlocks;
                    METRIC_FILE_SIZE_BUCKETS.observe(fileSize);
                }

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, GroupStats::new);
                synchronized (groupStat) {
                    groupStat.sumBlocks += fileBlocks;
                    METRIC_GROUP_FILE_SIZE_BUCKETS.labels(groupName).observe(fileSize);
                }

                // User stats
                final String userName = p.getUserName();
                UserStats user = report.userStats.computeIfAbsent(userName, UserStats::new);
                synchronized (user) {
                    METRIC_USER_FILE_SIZE_BUCKETS.labels(userName).observe(fileSize);
                    user.sumBlocks += fileBlocks;
                }
            }

            @Override
            public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, GroupStats::new);
                synchronized (groupStat) {
                    groupStat.sumDirectories++;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats user = report.userStats.computeIfAbsent(userName, UserStats::new);
                synchronized (user) {
                    user.sumDirectories++;
                }

                synchronized (overallStats) {
                    overallStats.sumDirectories++;
                }
            }

            @Override
            public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeSymlink d = inode.getSymlink();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, GroupStats::new);
                synchronized (groupStat) {
                    groupStat.sumSymLinks++;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats user = report.userStats.computeIfAbsent(userName, UserStats::new);
                synchronized (user) {
                    user.sumSymLinks++;
                }

                synchronized (overallStats) {
                    overallStats.sumSymLinks++;
                }
            }
        });
        return report;
    }

    public List<MetricFamilySamples> collect() {
        try (Gauge.Timer timer = METRIC_SCRAPE_DURATION.startTimer()) {
            METRIC_SCRAPE_REQUESTS.inc();
            scrape();

            final MemoryUsage heapMemoryUsage = memoryMXBean.getHeapMemoryUsage();
            METRIC_EXPORTER_HEAP.labels("max").set(heapMemoryUsage.getMax());
            METRIC_EXPORTER_HEAP.labels("used").set(heapMemoryUsage.getUsed());
        } catch (Exception e) {
            METRIC_SCRAPE_ERROR.inc();
            LOGGER.error("FSImage scrape failed", e);
        }

        return Collections.emptyList(); // Directly registered counters
    }
}

