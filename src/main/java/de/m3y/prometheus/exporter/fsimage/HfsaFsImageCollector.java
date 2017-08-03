package de.m3y.prometheus.exporter.fsimage;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import de.m3y.hadoop.hdfs.hfsa.util.SizeBucket;
import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
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
            .name(METRIC_PREFIX + "scrape_error_total")
            .help("Counts failed scrapes.").register();
    private static final Counter METRIC_SCRAPE_SKIPS = Counter.build()
            .name(METRIC_PREFIX + "scrape_skip_total")
            .help("Counts the fsimage scrapes skips, as the fsimage is versioned and got already processed.").register();
    private static final Gauge METRIC_SCRAPE_DURATION = Gauge.build()
            .name(METRIC_PREFIX + "scrape_duration_ms")
            .help("Scrape duration in ms").register();

    private String lastFsImageScraped = "";
    final Config config;

    /**
     * Sorts descending by file name.
     */
    public static final Comparator<File> FSIMAGE_FILENAME_COMPARATOR = new Comparator<File>() {
        @Override
        public int compare(File o1, File o2) {
            return o2.getName().compareTo(o1.getName());
        }
    };

    private final File fsImageDir;


    HfsaFsImageCollector(Config config) {
        final String path = config.getFsImagePath();
        if (null == path || path.isEmpty()) {
            throw new IllegalArgumentException("Must provide the directory location to the FSImage snapshots");
        }

        fsImageDir = new File(path);
        if (!fsImageDir.exists()) {
            throw new IllegalArgumentException(fsImageDir.getAbsolutePath() + " does not exist");
        }

        this.config = config;
    }

    static class FSImageFilenameFilter implements FilenameFilter {
        final static Pattern FS_IMAGE_PATTERN = Pattern.compile("fsimage_\\d+");

        @Override
        public boolean accept(File dir, String name) {
            return FS_IMAGE_PATTERN.matcher(name).matches();
        }
    }

    static final FilenameFilter FSIMAGE_FILTER = new FSImageFilenameFilter();

    private void scrape() throws IOException {
        // Check dir
        if (!fsImageDir.exists()) {
            throw new IllegalArgumentException(fsImageDir.getAbsolutePath() + " doest not exist");
        }

        final File[] files = fsImageDir.listFiles(FSIMAGE_FILTER);
        if (null == files || files.length == 0) {
            throw new IllegalStateException("No fsimage files found in " + fsImageDir.getAbsolutePath()
                    + " matching " + FSImageFilenameFilter.FS_IMAGE_PATTERN);
        }

        Arrays.sort(files, FSIMAGE_FILENAME_COMPARATOR);

        final File latestImageFile = files[0];
        // Skip, as metrics already reflect the info from previous scrape
        if (config.isSkipPreviouslyParsed() && lastFsImageScraped.equals(latestImageFile.getAbsolutePath())) {
            LOGGER.info("Skipping scrape of " + lastFsImageScraped
                    + ", as fsimage has already been previously processed");
            METRIC_SCRAPE_SKIPS.inc();
        } else {
            scrape(latestImageFile);
        }
        lastFsImageScraped = latestImageFile.getAbsolutePath();
    }

    static class UserStats {
        final String userName;
        long sumFiles;
        long sumBlocks;
        long sumFileSize;
        long sumDirectories;
        final SizeBucket fileSizeBuckets;

        UserStats(String userName) {
            this.userName = userName;
            fileSizeBuckets = new SizeBucket(new FixedBucketModel());
        }
    }

    static class GroupStats {
        final String groupName;
        long sumFiles;
        long sumBlocks;
        long sumFileSize;
        long sumDirectories;
        final SizeBucket fileSizeBuckets;

        GroupStats(String groupName) {
            this.groupName = groupName;
            fileSizeBuckets = new SizeBucket(new FixedBucketModel());
        }
    }

    static final Gauge METRIC_SUM_FILES = Gauge.build()
            .name(METRIC_PREFIX + "file_count")
            .help("Number of files.").register();
    static final Gauge METRIC_SUM_DIRS = Gauge.build()
            .name(METRIC_PREFIX + "dir_count")
            .help("Number of directories.").register();
    static final Gauge METRIC_SUM_BLOCKS = Gauge.build()
            .name(METRIC_PREFIX + "block_count")
            .help("Number of blocks.").register();
    static final Gauge METRIC_SUM_FILE_SIZE = Gauge.build()
            .name(METRIC_PREFIX + "file_size_bytes")
            .help("Sum of all file sizes.").register();

    // By user
    static final String METRIC_PREFIX_USER = METRIC_PREFIX + "user_";
    static final Gauge METRIC_USER_SUM_FILES = Gauge.build()
            .name(METRIC_PREFIX_USER + "file_count")
            .labelNames("user_name")
            .help("Number of files.").register();
    static final Gauge METRIC_USER_SUM_DIRS = Gauge.build()
            .name(METRIC_PREFIX_USER + "dir_count")
            .labelNames("user_name")
            .help("Number of directories.").register();
    static final Gauge METRIC_USER_SUM_BLOCKS = Gauge.build()
            .name(METRIC_PREFIX_USER + "block_count")
            .labelNames("user_name")
            .help("Number of blocks.").register();
    static final Gauge METRIC_USER_SUM_FILE_SIZE = Gauge.build()
            .name(METRIC_PREFIX_USER + "size_bytes")
            .labelNames("user_name")
            .help("Sum of all file sizes.").register();
    static final Gauge METRIC_USER_FILE_SIZE_BUCKETS_COUNT = Gauge.build()
            .name(METRIC_PREFIX_USER + "fsize_bucket_count")
            .labelNames("user_name", "bucket")
            .help("Counts file size distribution in buckets, showing small files and size distribution. " +
                    "Bucket label is upper size in bytes").register();

    // By group
    static final String METRIC_PREFIX_GROUP = METRIC_PREFIX + "group_";
    static final Gauge METRIC_GROUP_SUM_FILES = Gauge.build()
            .name(METRIC_PREFIX_GROUP + "file_count")
            .labelNames("group_name")
            .help("Number of files.").register();
    static final Gauge METRIC_GROUP_SUM_DIRS = Gauge.build()
            .name(METRIC_PREFIX_GROUP + "dir_count")
            .labelNames("group_name")
            .help("Number of directories.").register();
    static final Gauge METRIC_GROUP_SUM_BLOCKS = Gauge.build()
            .name(METRIC_PREFIX_GROUP + "block_count")
            .labelNames("group_name")
            .help("Number of blocks.").register();
    static final Gauge METRIC_GROUP_SUM_FILE_SIZE = Gauge.build()
            .name(METRIC_PREFIX_GROUP + "size_bytes")
            .labelNames("group_name")
            .help("Sum of all file sizes.").register();

    static class OverallStats {
        long sumFiles;
        long sumDirectories;
        long sumBlocks;
        long sumFileSize;
        final SizeBucket sizeBucket = new SizeBucket(new FixedBucketModel());
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

    private void scrape(File fsImageFile) throws IOException {
        LOGGER.info("Parsing " + fsImageFile.getName());

        RandomAccessFile raFile = new RandomAccessFile(fsImageFile, "r");
        final FSImageLoader loader = FSImageLoader.load(raFile);

        LOGGER.info("Visting ...");
        long start = System.currentTimeMillis();
        Report report = computeStats(loader);
        LOGGER.info("Visiting finished [" + (System.currentTimeMillis() - start) + "ms].");

        // Overall stats
        OverallStats overallStats = report.overallStats;
        METRIC_SUM_FILES.set(overallStats.sumFiles);
        METRIC_SUM_DIRS.set(overallStats.sumDirectories);
        METRIC_SUM_BLOCKS.set(overallStats.sumBlocks);
        METRIC_SUM_FILE_SIZE.set(overallStats.sumFileSize);

        // User stats
        for (UserStats userStat : report.userStats.values()) {
            String[] labelValues = new String[]{userStat.userName};
            METRIC_USER_SUM_FILES.labels(labelValues).set(userStat.sumFiles);
            METRIC_USER_SUM_DIRS.labels(labelValues).set(userStat.sumDirectories);
            METRIC_USER_SUM_BLOCKS.labels(labelValues).set(userStat.sumBlocks);
            METRIC_USER_SUM_FILE_SIZE.labels(labelValues).set(userStat.sumFileSize);

            final long[] bucketBorders = userStat.fileSizeBuckets.computeBucketUpperBorders();
            for (int i = 0; i < userStat.fileSizeBuckets.size(); i++) {
                String bucketLabel = Long.toString(bucketBorders[i]);
                METRIC_USER_FILE_SIZE_BUCKETS_COUNT.labels(userStat.userName, bucketLabel)
                        .set(userStat.fileSizeBuckets.getBucketCounter(i));
            }
        }

        // Group stats
        for (GroupStats groupStat : report.groupStats.values()) {
            String[] labelValues = new String[]{groupStat.groupName};
            METRIC_GROUP_SUM_FILES.labels(labelValues).set(groupStat.sumFiles);
            METRIC_GROUP_SUM_DIRS.labels(labelValues).set(groupStat.sumDirectories);
            METRIC_GROUP_SUM_BLOCKS.labels(labelValues).set(groupStat.sumBlocks);
            METRIC_GROUP_SUM_FILE_SIZE.labels(labelValues).set(groupStat.sumFileSize);
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
                    overallStats.sizeBucket.add(fileSize);
                    overallStats.sumBlocks += fileBlocks;
                    overallStats.sumFileSize += fileSize;
                    overallStats.sumFiles++;
                }

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, GroupStats::new);
                synchronized (groupStat) {
                    groupStat.sumFiles++;
                    groupStat.sumFileSize += fileSize;
                    groupStat.fileSizeBuckets.add(fileSize);
                    groupStat.sumBlocks += fileBlocks;
                }

                // User stats
                final String userName = p.getUserName();
                UserStats user = report.userStats.computeIfAbsent(userName, UserStats::new);
                synchronized (user) {
                    user.sumFiles++;
                    user.sumFileSize += fileSize;
                    user.fileSizeBuckets.add(fileSize);
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
                // TODO ?
            }
        });
        return report;
    }

    public List<MetricFamilySamples> collect() {
        long startTime = System.currentTimeMillis();
        METRIC_SCRAPE_REQUESTS.inc();
        try {
            scrape();
        } catch (Exception e) {
            METRIC_SCRAPE_ERROR.inc();
            LOGGER.warn("FSImage scrape failed", e);
        }

        METRIC_SCRAPE_DURATION.set((System.currentTimeMillis() - startTime));

        return Collections.emptyList(); // Directly registered counters
    }
}

