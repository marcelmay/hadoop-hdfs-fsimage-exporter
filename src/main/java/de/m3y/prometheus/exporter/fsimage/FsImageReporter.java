package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import de.m3y.hadoop.hdfs.hfsa.core.FsVisitor;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Histogram;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.server.namenode.FsImageProto;

/**
 * Generates a report for a loaded FSImage.
 */
public class FsImageReporter {

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

    static class Report {
        final Map<String, GroupStats> groupStats;
        final Histogram groupFileSizeDistribution = HfsaFsImageCollector.METRIC_GROUP_FILE_SIZE_BUCKETS_BUILDER.create();
        final Map<String, UserStats> userStats;
        final Histogram userFleSizeDistribution = HfsaFsImageCollector.METRIC_USER_FILE_SIZE_BUCKETS_BUILDER.create();
        final OverallStats overallStats;
        final Histogram overalFleSizeDistribution = HfsaFsImageCollector.METRIC_FILE_SIZE_BUCKETS_BUILDER.create();
        boolean error;

        Report() {
            groupStats = Collections.synchronizedMap(new HashMap<>());
            userStats = Collections.synchronizedMap(new HashMap<>());
            overallStats = new OverallStats();
        }

        public void unregister() {
            CollectorRegistry.defaultRegistry.unregister(groupFileSizeDistribution);
            CollectorRegistry.defaultRegistry.unregister(userFleSizeDistribution);
            CollectorRegistry.defaultRegistry.unregister(overalFleSizeDistribution);
        }

        public void register() {
            groupFileSizeDistribution.register();
            userFleSizeDistribution.register();
            overalFleSizeDistribution.register();
        }
    }

    private FsImageReporter() {
        // Nothing
    }

    static Report computeStatsReport(final FSImageLoader loader) throws IOException {
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
        return report;
    }
}
