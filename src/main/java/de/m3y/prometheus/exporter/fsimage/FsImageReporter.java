package de.m3y.prometheus.exporter.fsimage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Function;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.Summary;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.m3y.prometheus.exporter.fsimage.FsImageCollector.METRIC_PREFIX;
import static de.m3y.prometheus.exporter.fsimage.FsImageCollector.MetricFamilySamples;
import static de.m3y.prometheus.exporter.fsimage.FsImageUpdateHandler.FSIZE;
import static de.m3y.prometheus.exporter.fsimage.FsImageUpdateHandler.REPLICATION;

/**
 * Generates a report for a loaded FSImage.
 */
public class FsImageReporter {
    private static final Logger LOG = LoggerFactory.getLogger(FsImageReporter.class);

    interface MetricAdapter {
        void observe(long metricValue);

        long count();
    }

    static class HistogramMetricAdapter implements MetricAdapter {
        final Histogram.Child child;

        HistogramMetricAdapter(Histogram.Child child) {
            this.child = child;
        }

        @Override
        public void observe(long metricValue) {
            child.observe(metricValue);
        }

        @Override
        public long count() {
            return (long) sum(child.get().buckets);
        }

        private static double sum(double[] doubles) {
            double s = 0;
            for (double d : doubles) {
                s += d;
            }
            return s;
        }
    }

    static class SummaryMetricAdapter implements MetricAdapter {
        final Summary.Child child;

        SummaryMetricAdapter(Summary.Child child) {
            this.child = child;
        }

        @Override
        public void observe(long metricValue) {
            child.observe(metricValue);
        }

        @Override
        public long count() {
            return (long) child.get().count;
        }
    }

    abstract static class AbstractFileSystemStats {
        LongAdder sumDirectories = new LongAdder();
        LongAdder sumBlocks = new LongAdder();
        LongAdder sumSymLinks = new LongAdder();
        final MetricAdapter fileSize;

        protected AbstractFileSystemStats(MetricAdapter fileSize) {
            this.fileSize = fileSize;
        }
    }

    static class OverallStats extends AbstractFileSystemStats {
        final Summary replication;

        OverallStats(MetricAdapter fileSize, Summary replication) {
            super(fileSize);
            this.replication = replication;
        }
        // No additional attributes, for now.
    }

    static class UserStats extends AbstractFileSystemStats {
        final String userName;
        final MetricAdapter replication;

        UserStats(String userName, MetricAdapter fileSize, MetricAdapter replication) {
            super(fileSize);
            this.userName = userName;
            this.replication = replication;
        }
    }

    static class GroupStats extends AbstractFileSystemStats {
        final String groupName;

        GroupStats(String groupName, MetricAdapter fileSize) {
            super(fileSize);
            this.groupName = groupName;
        }
    }

    static class PathStats extends AbstractFileSystemStats {
        final String path;

        PathStats(String path, MetricAdapter fileSize) {
            super(fileSize);
            this.path = path;
        }
    }

    /**
     * Contains collected statistics for an FSImage.
     */
    static class Report {
        // Overall stats
        final OverallStats overallStats;
        final SimpleCollector overallFleSizeDistribution;
        final Summary overallReplication;
        // Group stats
        final Map<String, GroupStats> groupStats;
        final SimpleCollector groupFileSizeDistribution;
        final Function<String, GroupStats> createGroupStats;
        // User stats
        final Map<String, UserStats> userStats;
        final SimpleCollector userFileSizeDistribution;
        final Function<String, UserStats> createUserStat;
        final Summary userReplication;
        // Path stats
        final Map<String, PathStats> pathStats;
        final SimpleCollector<?> pathFileSizeDistribution;
        final Function<String, PathStats> createPathStat;
        boolean error = false;
        // Path sets
        final Map<String, PathStats> pathSetStats;
        final SimpleCollector pathSetFileSizeDistribution;
        final Function<String, PathStats> createPathSetStat;

        Report(Config config) {
            groupStats = new ConcurrentHashMap<>();
            userStats = new ConcurrentHashMap<>();
            pathStats = new ConcurrentHashMap<>();
            pathSetStats = new ConcurrentHashMap<>();

            double[] configuredBuckets = config.getFileSizeDistributionBucketsAsDoubles();

            // Overall
            Histogram overallHistogram = Histogram.build()
                    .name(METRIC_PREFIX + FSIZE)
                    .buckets(configuredBuckets)
                    .help("Overall file size distribution")
                    .create();
            overallFleSizeDistribution = overallHistogram;
            overallReplication = Summary.build()
                    .name(FsImageCollector.METRIC_PREFIX + REPLICATION)
                    .help("Overall file replication").create();
            overallStats = new OverallStats(new HistogramMetricAdapter(overallHistogram.labels()), overallReplication);

            // Group
            if (config.isSkipFileDistributionForGroupStats()) {
                Summary summary = Summary.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_GROUP + FSIZE)
                        .labelNames(FsImageUpdateHandler.LABEL_GROUP_NAME)
                        .help("Per group file size and file count").create();
                createGroupStats = groupName -> new GroupStats(groupName,
                        new SummaryMetricAdapter(summary.labels(groupName)));
                groupFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_GROUP + FSIZE)
                        .labelNames(FsImageUpdateHandler.LABEL_GROUP_NAME)
                        .buckets(configuredBuckets)
                        .help("Per group file size distribution.").create();
                createGroupStats = groupName -> new GroupStats(groupName,
                        new HistogramMetricAdapter(histogram.labels(groupName)));
                groupFileSizeDistribution = histogram;
            }

            // User
            userReplication = Summary.build()
                    .name(FsImageUpdateHandler.METRIC_PREFIX_USER + REPLICATION)
                    .labelNames(FsImageUpdateHandler.LABEL_USER_NAME)
                    .help("Per user file replication").create();
            if (config.isSkipFileDistributionForUserStats()) {
                Summary summary = Summary.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_USER + FSIZE)
                        .labelNames(FsImageUpdateHandler.LABEL_USER_NAME)
                        .help("Per user file size and file count").create();
                createUserStat = userName -> new UserStats(userName,
                        new SummaryMetricAdapter(summary.labels(userName)),
                        new SummaryMetricAdapter(userReplication.labels(userName)));
                userFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_USER + FSIZE)
                        .labelNames(FsImageUpdateHandler.LABEL_USER_NAME)
                        .buckets(configuredBuckets)
                        .help("Per user file size distribution").create();
                createUserStat = userName -> new UserStats(userName,
                        new HistogramMetricAdapter(histogram.labels(userName)),
                        new SummaryMetricAdapter(userReplication.labels(userName)));
                userFileSizeDistribution = histogram;
            }

            // Paths
            if (config.isSkipFileDistributionForPathStats()) {
                Summary summary = Summary.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_PATH + FSIZE)
                        .labelNames(FsImageUpdateHandler.LABEL_PATH)
                        .help("Path specific file size and file count").create();
                createPathStat = path -> new PathStats(path, new SummaryMetricAdapter(summary.labels(path)));
                pathFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_PATH + FSIZE)
                        .buckets(configuredBuckets)
                        .labelNames(FsImageUpdateHandler.LABEL_PATH)
                        .help("Path specific file size distribution").create();
                createPathStat = path -> new PathStats(path, new HistogramMetricAdapter(histogram.labels(path)));
                pathFileSizeDistribution = histogram;
            }

            // Path sets
            if (config.isSkipFileDistributionForPathSetStats()) {
                Summary summary = Summary.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_PATH_SET + FSIZE)
                        .labelNames(FsImageUpdateHandler.LABEL_PATH_SET)
                        .help("Path set specific file size and file count").create();
                createPathSetStat = path -> new PathStats(path, new SummaryMetricAdapter(summary.labels(path)));
                pathSetFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(FsImageUpdateHandler.METRIC_PREFIX_PATH_SET + FSIZE)
                        .buckets(configuredBuckets)
                        .labelNames(FsImageUpdateHandler.LABEL_PATH_SET)
                        .help("Path set specific file size distribution").create();
                createPathSetStat = path -> new PathStats(path, new HistogramMetricAdapter(histogram.labels(path)));
                pathSetFileSizeDistribution = histogram;
            }
        }

        public void collect(List<MetricFamilySamples> mfs) {
            mfs.addAll(overallFleSizeDistribution.collect());
            mfs.addAll(overallReplication.collect());

            mfs.addAll(groupFileSizeDistribution.collect());

            mfs.addAll(userFileSizeDistribution.collect());
            mfs.addAll(userReplication.collect());

            if (hasPathStats()) {
                mfs.addAll(pathFileSizeDistribution.collect());
            }
            if (hasPathSetStats()) {
                mfs.addAll(pathSetFileSizeDistribution.collect());
            }
        }

        boolean hasPathStats() {
            return null != pathStats && !pathStats.isEmpty();
        }

        boolean hasPathSetStats() {
            return null != pathSetStats && !pathSetStats.isEmpty();
        }

    }

    private FsImageReporter() {
        // Nothing
    }

    static Report computeStatsReport(final FSImageLoader loader, Config config) throws IOException {
        Report report = new Report(config);
        final OverallStats overallStats = report.overallStats;

        long t = System.currentTimeMillis();
        loader.visitParallel(new FsVisitor() {
            @Override
            public void onFile(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeFile f = inode.getFile();
                PermissionStatus p = loader.getPermissionStatus(f.getPermission());

                final long fileSize = FSImageLoader.getFileSize(f);
                final long fileBlocks = f.getBlocksCount();
                overallStats.sumBlocks.add(fileBlocks);
                overallStats.fileSize.observe(fileSize);
                overallStats.replication.observe(f.getReplication());

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, report.createGroupStats);
                groupStat.sumBlocks.add(fileBlocks);
                groupStat.fileSize.observe(fileSize);

                // User stats
                final String userName = p.getUserName();
                UserStats userStat = report.userStats.computeIfAbsent(userName, report.createUserStat);
                userStat.sumBlocks.add(fileBlocks);
                userStat.fileSize.observe(fileSize);
                userStat.replication.observe(f.getReplication());
            }

            @Override
            public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeDirectory d = inode.getDirectory();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Visiting directory {}",
                            path + ("/".equals(path) ? "" : "/") + inode.getName().toStringUtf8());
                }

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, report.createGroupStats);
                groupStat.sumDirectories.increment();

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.userStats.computeIfAbsent(userName, report.createUserStat);
                userStat.sumDirectories.increment();

                overallStats.sumDirectories.increment();
            }

            @Override
            public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
                FsImageProto.INodeSection.INodeSymlink d = inode.getSymlink();
                PermissionStatus p = loader.getPermissionStatus(d.getPermission());

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, report.createGroupStats);
                groupStat.sumSymLinks.increment();

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.userStats.computeIfAbsent(userName, report.createUserStat);
                userStat.sumSymLinks.increment();

                overallStats.sumSymLinks.increment();
            }
        });
        LOG.info("Finished computing overall/group/user stats in {}ms", System.currentTimeMillis() - t);
        if (config.hasPaths()) {
            computePathStats(loader, config, report);
        }
        if (config.hasPathSets()) {
            computePathSetStatsParallel(loader, config, report);
        }

        return report;
    }

    private static void computePathStats(FSImageLoader loader, Config config, Report report) throws IOException {
        Set<String> expandedPaths = expandPaths(loader, config.getPaths());
        LOG.info("Expanded paths {} for path stats {}", expandedPaths, config.getPaths());
        long s = System.currentTimeMillis();
        expandedPaths.parallelStream().forEach(p -> {
            try {
                long t = System.currentTimeMillis();
                final PathStats pathStats = report.pathStats.computeIfAbsent(p, report.createPathStat);
                loader.visit(new PathStatVisitor(pathStats), p);
                // Subtract start dir, as only child dirs count
                pathStats.sumDirectories.decrement();
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Finished path stat for {} with {} total number of files in {}ms",
                            p, pathStats.fileSize.count(), System.currentTimeMillis() - t);
                }
            } catch (IOException e) {
                LOG.error("Can not traverse " + p, e);
                report.error = true;
            }
        });
        LOG.info("Finished {} path stats in {}ms", report.pathStats.size(), System.currentTimeMillis() - s);
    }

    private static void computePathSetStatsParallel(FSImageLoader loader, Config config, Report report) {
        long s = System.currentTimeMillis();
        config.getPathSets().entrySet().parallelStream().forEach(entry ->
                computePathSetStats(loader, entry, report)
        );
        LOG.info("Finished {} path set stats in {}ms", report.pathSetStats.size(), System.currentTimeMillis() - s);
    }

    private static void computePathSetStats(FSImageLoader loader, Map.Entry<String, List<String>> entry, Report report) {
        try {
            Set<String> expandedPaths = expandPaths(loader, entry.getValue());
            LOG.info("Expanded paths {} for path set stats {}", expandedPaths, entry.getKey());
            long t = System.currentTimeMillis();
            final PathStats pathStats = report.pathSetStats.computeIfAbsent(entry.getKey(), report.createPathSetStat);
            final PathStatVisitor visitor = new PathStatVisitor(pathStats);
            for (String path : expandedPaths) {
                loader.visit(visitor, path);
            }
            // Subtract number of start dirs, as only child dirs count
            pathStats.sumDirectories.add(-expandedPaths.size());
            if (LOG.isDebugEnabled()) {
                LOG.debug("Finished path set stat for {} with {} total number of files in {}ms",
                        entry.getKey(), pathStats.fileSize.count(), System.currentTimeMillis() - t);
            }
        } catch (IOException e) {
            LOG.error("Can not traverse path set " + entry.getKey() + " using paths " + entry.getValue(), e);
            report.error = true;
        }
    }

    static Set<String> expandPaths(FSImageLoader loader, Collection<String> paths) throws IOException {
        Set<String> expandedPaths = new HashSet<>();
        for (String path : paths) {
            // If path does not exist, match child directories
            if (!hasDirectory(loader, path)) {
                addMatchingPaths(loader, expandedPaths, path);
            } else { // Existing directory
                expandedPaths.add(path);
            }
        }

        return expandedPaths;
    }

    private static void addMatchingPaths(FSImageLoader loader, Set<String> expandedPaths, String path)
            throws IOException {
        int idx = path.lastIndexOf('/');
        if (idx < 0) {
            LOG.error("Skipping invalid path <{}> for per-path-stats", path);
        } else {
            String parent = (idx == 0 ? "/" : path.substring(0, idx));
            try {
                List<String> childPaths = loader.getChildPaths(parent);
                Pattern pattern = Pattern.compile(path);
                childPaths.removeIf(p -> !pattern.matcher(p).matches());
                if (childPaths.isEmpty()) {
                    LOG.warn("No matches found for {}", path);
                } else {
                    expandedPaths.addAll(childPaths);
                }
            } catch(FileNotFoundException ex) {
                LOG.warn("Skipping configured, non-existing path {} for metric computations." +
                        " Check your configuration path/pathSet entries!", parent);
            }
        }
    }

    /**
     * TODO: Replace once FSImageLoader contains this functionality
     */
    private static boolean hasDirectory(FSImageLoader loader, String path) throws IOException {
        if ("/".equals(path)) { // Root always exists
            return true;
        }
        try {
            return null != loader.getINodeFromPath(path);
        } catch (FileNotFoundException | IllegalArgumentException ex) {
            return false;
        }
    }


    static class PathStatVisitor implements FsVisitor {
        private final PathStats pathStats;

        PathStatVisitor(PathStats pathStats) {
            this.pathStats = pathStats;
        }

        @Override
        public void onFile(FsImageProto.INodeSection.INode inode, String path) {
            FsImageProto.INodeSection.INodeFile f = inode.getFile();
            pathStats.sumBlocks.add(f.getBlocksCount());
            final long fileSize = FSImageLoader.getFileSize(f);
            pathStats.fileSize.observe(fileSize);
        }

        @Override
        public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
            pathStats.sumDirectories.increment();
        }

        @Override
        public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
            pathStats.sumSymLinks.increment();
        }
    }

}
