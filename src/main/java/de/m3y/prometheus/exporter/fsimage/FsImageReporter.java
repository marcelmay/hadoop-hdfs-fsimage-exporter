package de.m3y.prometheus.exporter.fsimage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generates a report for a loaded FSImage.
 */
public class FsImageReporter {
    private static final Logger LOG = LoggerFactory.getLogger(FSImageLoader.class);

    abstract static class AbstractFileSizeAwareStats {
        long sumDirectories;
        long sumBlocks;
        long sumSymLinks;
    }

    static class OverallStats extends AbstractFileSizeAwareStats {
        // No additional attributes, for now.
    }

    static class UserStats extends AbstractFileSizeAwareStats {
        final String userName;

        UserStats(String userName) {
            this.userName = userName;
        }
    }

    static class GroupStats extends AbstractFileSizeAwareStats {
        final String groupName;

        GroupStats(String groupName) {
            this.groupName = groupName;
        }
    }

    static class PathStats extends AbstractFileSizeAwareStats {
        final String path;
        final Histogram.Child fileSizeDistributionChild;

        PathStats(String path, Histogram fileSizeDistribution) {
            this.path = path;
            fileSizeDistributionChild = fileSizeDistribution.labels(path);
        }
    }

    /**
     * Contains collected statistics.
     * <p>
     * Note: Uses histogram metrics, as precomputed values can not be set on Prometheus histogram directly.
     */
    static class Report {
        final Map<String, GroupStats> groupStats;
        final Histogram groupFileSizeDistribution = HfsaFsImageCollector.METRIC_GROUP_FILE_SIZE_BUCKETS_BUILDER.create();
        final Map<String, UserStats> userStats;
        final Histogram userFleSizeDistribution = HfsaFsImageCollector.METRIC_USER_FILE_SIZE_BUCKETS_BUILDER.create();
        final OverallStats overallStats;
        final Histogram overalFleSizeDistribution = HfsaFsImageCollector.METRIC_FILE_SIZE_BUCKETS_BUILDER.create();
        final Map<String, PathStats> pathStats;
        final Histogram pathFileSizeDistribution = HfsaFsImageCollector.METRIC_PATH_FILE_SIZE_BUCKETS_BUILDER.create();
        boolean error;

        Report() {
            groupStats = Collections.synchronizedMap(new HashMap<>());
            userStats = Collections.synchronizedMap(new HashMap<>());
            pathStats = Collections.synchronizedMap(new HashMap<>());
            overallStats = new OverallStats();
        }

        public void unregister() {
            CollectorRegistry.defaultRegistry.unregister(groupFileSizeDistribution);
            CollectorRegistry.defaultRegistry.unregister(userFleSizeDistribution);
            CollectorRegistry.defaultRegistry.unregister(overalFleSizeDistribution);
            if (hasPathStats()) {
                CollectorRegistry.defaultRegistry.unregister(pathFileSizeDistribution);
            }
        }

        public void register() {
            groupFileSizeDistribution.register();
            userFleSizeDistribution.register();
            overalFleSizeDistribution.register();
            if (hasPathStats()) {
                pathFileSizeDistribution.register();
            }
        }

        boolean hasPathStats() {
            return null != pathStats && !pathStats.isEmpty();
        }
    }

    private FsImageReporter() {
        // Nothing
    }

    static Report computeStatsReport(final FSImageLoader loader, Config config) throws IOException {
        Report report = new Report();
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
                }
                report.overalFleSizeDistribution.observe(fileSize);

                // Group stats
                final String groupName = p.getGroupName();
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, GroupStats::new);
                synchronized (groupStat) {
                    groupStat.sumBlocks += fileBlocks;
                }
                report.groupFileSizeDistribution.labels(groupName).observe(fileSize);

                // User stats
                final String userName = p.getUserName();
                UserStats userStat = report.userStats.computeIfAbsent(userName, UserStats::new);
                synchronized (userStat) {
                    userStat.sumBlocks += fileBlocks;
                }
                report.userFleSizeDistribution.labels(userName).observe(fileSize);
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
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, GroupStats::new);
                synchronized (groupStat) {
                    groupStat.sumDirectories++;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.userStats.computeIfAbsent(userName, UserStats::new);
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
                final GroupStats groupStat = report.groupStats.computeIfAbsent(groupName, GroupStats::new);
                synchronized (groupStat) {
                    groupStat.sumSymLinks++;
                }

                // User stats
                final String userName = p.getUserName();
                final UserStats userStat = report.userStats.computeIfAbsent(userName, UserStats::new);
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

        return report;
    }

    private static void computePathStats(FSImageLoader loader, Config config, Report report) throws IOException {
        Set<String> expandedPaths = expandPaths(loader, config.getPaths());
        LOG.info("Expanded paths {} for path stats {}", expandedPaths, config.getPaths());
        long s = System.currentTimeMillis();
        expandedPaths.parallelStream().forEach(p -> {
            try {
                long t = System.currentTimeMillis();
                final PathStats pathStats = report.pathStats.computeIfAbsent(p,
                        path -> new PathStats(path, report.pathFileSizeDistribution));
                loader.visit(new PathStatVisitor(pathStats), p);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Finished path stat for {} with {} total number of files in {}ms",
                            p, (long) sum(pathStats.fileSizeDistributionChild.get().buckets), System.currentTimeMillis() - t);
                }
            } catch (IOException e) {
                LOG.error("Can not traverse " + p, e);
                report.error = true;
            }
        });
        LOG.info("Finished {} path stats in {}ms", report.pathStats.size(), System.currentTimeMillis() - s);
    }

    static Set<String> expandPaths(FSImageLoader loader, Set<String> paths) throws IOException {
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

    private static double sum(double[] doubles) {
        double s = 0;
        for (double d : doubles) {
            s += d;
        }
        return s;
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
            pathStats.fileSizeDistributionChild.observe(fileSize);
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
