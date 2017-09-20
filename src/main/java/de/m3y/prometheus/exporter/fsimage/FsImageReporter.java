package de.m3y.prometheus.exporter.fsimage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import io.prometheus.client.SimpleCollector;
import io.prometheus.client.Summary;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a report for a loaded FSImage.
 */
public class FsImageReporter {
    private static final Logger LOG = LoggerFactory.getLogger(FSImageLoader.class);

    interface FileSizeMetricAdapter {
        void observeFileSize(long fileSize);

        long count();
    }

    static class HistogramFileSizeMetricAdapter implements FileSizeMetricAdapter {
        final Histogram.Child fileSizeDistributionChild;

        HistogramFileSizeMetricAdapter(Histogram.Child fileSizeDistributionChild) {
            this.fileSizeDistributionChild = fileSizeDistributionChild;
        }

        @Override
        public void observeFileSize(long fileSize) {
            fileSizeDistributionChild.observe(fileSize);
        }

        @Override
        public long count() {
            return (long) sum(fileSizeDistributionChild.get().buckets);
        }

        private static double sum(double[] doubles) {
            double s = 0;
            for (double d : doubles) {
                s += d;
            }
            return s;
        }
    }

    static class SummaryFileSizeMetricAdapter implements FileSizeMetricAdapter {
        final Summary.Child fileSizeDistributionChild;

        SummaryFileSizeMetricAdapter(Summary.Child fileSizeDistributionChild) {
            this.fileSizeDistributionChild = fileSizeDistributionChild;
        }

        @Override
        public void observeFileSize(long fileSize) {
            fileSizeDistributionChild.observe(fileSize);
        }

        @Override
        public long count() {
            return (long) fileSizeDistributionChild.get().count;
        }
    }

    abstract static class AbstractFileSizeAwareStats {
        long sumDirectories;
        long sumBlocks;
        long sumSymLinks;
        final FileSizeMetricAdapter fileSize;

        protected AbstractFileSizeAwareStats(FileSizeMetricAdapter fileSize) {
            this.fileSize = fileSize;
        }
    }

    static class OverallStats extends AbstractFileSizeAwareStats {
        OverallStats(FileSizeMetricAdapter fileSize) {
            super(fileSize);
        }
        // No additional attributes, for now.
    }

    static class UserStats extends AbstractFileSizeAwareStats {
        final String userName;

        UserStats(String userName, FileSizeMetricAdapter fileSize) {
            super(fileSize);
            this.userName = userName;
        }
    }

    static class GroupStats extends AbstractFileSizeAwareStats {
        final String groupName;

        GroupStats(String groupName, FileSizeMetricAdapter fileSize) {
            super(fileSize);
            this.groupName = groupName;
        }
    }

    static class PathStats extends AbstractFileSizeAwareStats {
        final String path;

        PathStats(String path, FileSizeMetricAdapter fileSize) {
            super(fileSize);
            this.path = path;
        }
    }

    /**
     * Contains collected statistics.
     * <p>
     * Note: Uses histogram metrics, as precomputed values can not be set on Prometheus histogram directly.
     */
    static class Report {
        final OverallStats overallStats;
        final SimpleCollector overallFleSizeDistribution;
        final Map<String, GroupStats> groupStats;
        final SimpleCollector groupFileSizeDistribution;
        final Function<String, GroupStats> createGroupStats;
        final Map<String, UserStats> userStats;
        final SimpleCollector userFileSizeDistribution;
        final Function<String, UserStats> createUserStat;
        final Map<String, PathStats> pathStats;
        final SimpleCollector pathFileSizeDistribution;
        final Function<String, PathStats> createPathStat;
        boolean error;
        // PAth sets
        final Map<String, PathStats> pathSetStats;
        final SimpleCollector pathSetFileSizeDistribution;
        final Function<String, PathStats> createPathSetStat;

        Report(Config config) {
            groupStats = Collections.synchronizedMap(new HashMap<>());
            userStats = Collections.synchronizedMap(new HashMap<>());
            pathStats = Collections.synchronizedMap(new HashMap<>());
            pathSetStats = Collections.synchronizedMap(new HashMap<>());

            // Overall
            Histogram overallHistogram = HfsaFsImageCollector.METRIC_FILE_SIZE_BUCKETS_BUILDER.create();
            overallFleSizeDistribution = overallHistogram;
            overallStats = new OverallStats(new HistogramFileSizeMetricAdapter(overallHistogram.labels()));

            // Group
            if (config.skipFileDistributionForGroupStats) {
                Summary summary = Summary.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_GROUP + HfsaFsImageCollector.FSIZE)
                        .labelNames(HfsaFsImageCollector.LABEL_GROUP_NAME)
                        .help("Per group file size and file count").create();
                createGroupStats = groupName -> new GroupStats(groupName, new SummaryFileSizeMetricAdapter(summary.labels(groupName)));
                groupFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_GROUP + HfsaFsImageCollector.FSIZE)
                        .labelNames(HfsaFsImageCollector.LABEL_GROUP_NAME)
                        .buckets(Arrays.stream(HfsaFsImageCollector.BUCKET_UPPER_BOUNDARIES).asDoubleStream().toArray())
                        .help("Per group file size distribution.").create();
                createGroupStats = groupName -> new GroupStats(groupName, new HistogramFileSizeMetricAdapter(histogram.labels(groupName)));
                groupFileSizeDistribution = histogram;
            }

            // User
            if (config.skipFileDistributionForUserStats) {
                Summary summary = Summary.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_USER + HfsaFsImageCollector.FSIZE)
                        .labelNames(HfsaFsImageCollector.LABEL_USER_NAME)
                        .help("Per user file size and file count").create();
                createUserStat = userName -> new UserStats(userName, new SummaryFileSizeMetricAdapter(summary.labels(userName)));
                userFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_USER + HfsaFsImageCollector.FSIZE)
                        .labelNames(HfsaFsImageCollector.LABEL_USER_NAME)
                        .buckets(Arrays.stream(HfsaFsImageCollector.BUCKET_UPPER_BOUNDARIES).asDoubleStream().toArray())
                        .help("Per user file size distribution").create();
                createUserStat = userName -> new UserStats(userName, new HistogramFileSizeMetricAdapter(histogram.labels(userName)));
                userFileSizeDistribution = histogram;
            }

            // Paths
            if (config.skipFileDistributionForPathStats) {
                Summary summary = Summary.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_PATH + HfsaFsImageCollector.FSIZE)
                        .labelNames(HfsaFsImageCollector.LABEL_PATH)
                        .help("Path specific file size and file count").create();
                createPathStat = path -> new PathStats(path, new SummaryFileSizeMetricAdapter(summary.labels(path)));
                pathFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_PATH + HfsaFsImageCollector.FSIZE)
                        .buckets(Arrays.stream(HfsaFsImageCollector.BUCKET_UPPER_BOUNDARIES).asDoubleStream().toArray())
                        .labelNames(HfsaFsImageCollector.LABEL_PATH)
                        .help("Path specific file size distribution").create();
                createPathStat = path -> new PathStats(path, new HistogramFileSizeMetricAdapter(histogram.labels(path)));
                pathFileSizeDistribution = histogram;
            }

            // Path sets
            if (config.skipFileDistributionForPathSetStats) {
                Summary summary = Summary.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_PATH_SET + HfsaFsImageCollector.FSIZE)
                        .labelNames(HfsaFsImageCollector.LABEL_PATH_SET)
                        .help("Path set specific file size and file count").create();
                createPathSetStat = path -> new PathStats(path, new SummaryFileSizeMetricAdapter(summary.labels(path)));
                pathSetFileSizeDistribution = summary;
            } else {
                Histogram histogram = Histogram.build()
                        .name(HfsaFsImageCollector.METRIC_PREFIX_PATH_SET + HfsaFsImageCollector.FSIZE)
                        .buckets(Arrays.stream(HfsaFsImageCollector.BUCKET_UPPER_BOUNDARIES).asDoubleStream().toArray())
                        .labelNames(HfsaFsImageCollector.LABEL_PATH_SET)
                        .help("Path set specific file size distribution").create();
                createPathSetStat = path -> new PathStats(path, new HistogramFileSizeMetricAdapter(histogram.labels(path)));
                pathSetFileSizeDistribution = histogram;
            }
        }

        public void unregister() {
            CollectorRegistry.defaultRegistry.unregister(groupFileSizeDistribution);
            CollectorRegistry.defaultRegistry.unregister(userFileSizeDistribution);
            CollectorRegistry.defaultRegistry.unregister(overallFleSizeDistribution);
            if (hasPathStats()) {
                CollectorRegistry.defaultRegistry.unregister(pathFileSizeDistribution);
            }
            if (hasPathSetStats()) {
                CollectorRegistry.defaultRegistry.unregister(pathSetFileSizeDistribution);
            }
        }

        public void register() {
            groupFileSizeDistribution.register();
            userFileSizeDistribution.register();
            overallFleSizeDistribution.register();
            if (hasPathStats()) {
                pathFileSizeDistribution.register();
            }
            if (hasPathSetStats()) {
                pathSetFileSizeDistribution.register();
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
                synchronized (overallStats) {
                    overallStats.sumBlocks += fileBlocks;
                    overallStats.fileSize.observeFileSize(fileSize);
                }

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, report.createGroupStats);
                synchronized (groupStat) {
                    groupStat.sumBlocks += fileBlocks;
                    groupStat.fileSize.observeFileSize(fileSize);
                }

                // User stats
                final String userName = p.getUserName();
                UserStats userStat = report.userStats.computeIfAbsent(userName, report.createUserStat);
                synchronized (userStat) {
                    userStat.sumBlocks += fileBlocks;
                    userStat.fileSize.observeFileSize(fileSize);
                }
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
                synchronized (groupStat) {
                    groupStat.sumDirectories++;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.userStats.computeIfAbsent(userName, report.createUserStat);
                synchronized (userStat) {
                    userStat.sumDirectories++;
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
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, report.createGroupStats);
                synchronized (groupStat) {
                    groupStat.sumSymLinks++;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.userStats.computeIfAbsent(userName, report.createUserStat);
                synchronized (userStat) {
                    userStat.sumSymLinks++;
                }

                synchronized (overallStats) {
                    overallStats.sumSymLinks++;
                }
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
                pathStats.sumDirectories -= 1;
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
                computePathSetStats(loader, entry, config, report)
        );
        LOG.info("Finished {} path set stats in {}ms", report.pathSetStats.size(), System.currentTimeMillis() - s);
    }

    private static void computePathSetStats(FSImageLoader loader, Map.Entry<String, List<String>> entry, Config config, Report report) {
        try {
            Set<String> expandedPaths = expandPaths(loader, entry.getValue());
            LOG.info("Expanded paths {} for path set stats {}", expandedPaths, config.getPaths());
            long t = System.currentTimeMillis();
            final PathStats pathStats = report.pathSetStats.computeIfAbsent(entry.getKey(), report.createPathSetStat);
            final PathStatVisitor visitor = new PathStatVisitor(pathStats);
            for (String path : expandedPaths) {
                loader.visit(visitor, path);
            }
            // Subtract number of start dirs, as only child dirs count
            pathStats.sumDirectories -= expandedPaths.size();
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
            List<String> childPaths = loader.getChildPaths(parent);
            Pattern pattern = Pattern.compile(path);
            childPaths.removeIf(p -> !pattern.matcher(p).matches());
            if (childPaths.isEmpty()) {
                LOG.warn("No matches found for {}", path);
            } else {
                expandedPaths.addAll(childPaths);
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
            pathStats.sumBlocks += f.getBlocksCount();
            final long fileSize = FSImageLoader.getFileSize(f);
            pathStats.fileSize.observeFileSize(fileSize);
        }

        @Override
        public void onDirectory(FsImageProto.INodeSection.INode inode, String path) {
            pathStats.sumDirectories++;
        }

        @Override
        public void onSymLink(FsImageProto.INodeSection.INode inode, String path) {
            pathStats.sumSymLinks++;
        }
    }
}
