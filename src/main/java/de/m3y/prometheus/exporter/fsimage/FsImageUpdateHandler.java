package de.m3y.prometheus.exporter.fsimage;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import io.prometheus.client.Collector;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads FSImage and computes metrics.
 * <p>
 * {@link FsImageWatcher} checks for FSImage file updates and triggers {@link FsImageUpdateHandler#onFsImageChange(File)}.
 */
class FsImageUpdateHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(FsImageUpdateHandler.class);
    private static final String METRIC_POSTFIX_DIRS = "dirs";
    private static final String METRIC_POSTFIX_BLOCKS = "blocks";
    private static final String METRIC_POSTFIX_LINKS = "links";
    static final String FSIZE = "fsize";
    static final String REPLICATION = "replication";
    static final String LABEL_USER_NAME = "user_name";

    private static final String HELP_NUMBER_OF_SYM_LINKS = "Number of sym links.";
    private static final String HELP_NUMBER_OF_DIRECTORIES = "Number of directories.";
    private static final String HELP_NUMBER_OF_BLOCKS = "Number of blocks.";

    static class FsMetrics {
        private static final String[] EMPTY_LABEL_NAMES = new String[]{};
        private final Gauge sumDirs;
        private final Gauge sumLinks;
        private final Gauge sumBlocks;

        FsMetrics(String prefix, String[] labelNames) {
            sumDirs = Gauge.build()
                    .name(prefix + METRIC_POSTFIX_DIRS)
                    .labelNames(labelNames)
                    .help(HELP_NUMBER_OF_DIRECTORIES).create();
            sumLinks = Gauge.build()
                    .name(prefix + METRIC_POSTFIX_LINKS)
                    .labelNames(labelNames)
                    .help(HELP_NUMBER_OF_SYM_LINKS).create();
            sumBlocks = Gauge.build()
                    .name(prefix + METRIC_POSTFIX_BLOCKS)
                    .labelNames(labelNames)
                    .help(HELP_NUMBER_OF_BLOCKS).create();
        }

        FsMetrics(String prefix) {
            this(prefix, EMPTY_LABEL_NAMES);
        }

        void update(FsImageReporter.AbstractFileSystemStats fsStats, String... labelValues) {
            if (null == labelValues || labelValues.length == 0) {
                sumDirs.set(fsStats.sumDirectories.doubleValue());
                sumLinks.set(fsStats.sumSymLinks.doubleValue());
                sumBlocks.set(fsStats.sumBlocks.doubleValue());
            } else {
                sumDirs.labels(labelValues).set(fsStats.sumDirectories.doubleValue());
                sumLinks.labels(labelValues).set(fsStats.sumSymLinks.doubleValue());
                sumBlocks.labels(labelValues).set(fsStats.sumBlocks.doubleValue());
            }
        }

        void collect(List<Collector.MetricFamilySamples> mfs) {
            mfs.addAll(sumDirs.collect());
            mfs.addAll(sumBlocks.collect());
            mfs.addAll(sumLinks.collect());
        }
    }

    private final FsMetrics overall = new FsMetrics(FsImageCollector.METRIC_PREFIX);

    // By user
    static final String METRIC_PREFIX_USER = FsImageCollector.METRIC_PREFIX + "user_";
    private final FsMetrics userFsMetrics = new FsMetrics(FsImageCollector.METRIC_PREFIX + "user_",
            new String[]{LABEL_USER_NAME});

    // By group
    static final String METRIC_PREFIX_GROUP = FsImageCollector.METRIC_PREFIX + "group_";
    static final String LABEL_GROUP_NAME = "group_name";
    private final FsMetrics groupFsMetrics = new FsMetrics(METRIC_PREFIX_GROUP,
            new String[]{LABEL_GROUP_NAME});

    // By path
    static final String METRIC_PREFIX_PATH = FsImageCollector.METRIC_PREFIX + "path_";
    static final String LABEL_PATH = "path";
    private final FsMetrics pathFsMetrics = new FsMetrics(METRIC_PREFIX_PATH,
            new String[]{LABEL_PATH});

    // By path set
    static final String METRIC_PREFIX_PATH_SET = FsImageCollector.METRIC_PREFIX + "path_set_";
    static final String LABEL_PATH_SET = "path_set";
    private final FsMetrics pathSetFsMetrics = new FsMetrics(METRIC_PREFIX_PATH_SET,
            new String[]{LABEL_PATH_SET});

    private final Summary metricLoadDuration = Summary.build()
            .name(FsImageCollector.METRIC_PREFIX + "load_duration_seconds")
            .help("Time for loading/parsing FSImage").create();
    private final Summary metricVisitDuration = Summary.build()
            .name(FsImageCollector.METRIC_PREFIX + "compute_stats_duration_seconds")
            .help("Time for computing stats for a loaded FSImage").create();
    private final Gauge metricLoadSize = Gauge.build()
            .name(FsImageCollector.METRIC_PREFIX + "load_file_size_bytes")
            .help("Size of raw FSImage").create();

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition reportUpdated = lock.newCondition();
    private FsImageReporter.Report report;
    private FsImageReporter.Report currentReport;
    private Config config;

    public FsImageUpdateHandler(Config config) {
        this.config = config;
    }

    /**
     * Collects MFS.
     *
     * @param mfs the sampled metrics
     * @return true if error occurred
     */
    public boolean collectFsImageSamples(List<Collector.MetricFamilySamples> mfs) {
        // Switch report
        synchronized (this) {
            final FsImageReporter.Report latestReport = getFsImageReport();
            if (latestReport != currentReport) {
                currentReport = latestReport;
            }
        }

        mfs.addAll(metricLoadDuration.collect());
        mfs.addAll(metricVisitDuration.collect());
        mfs.addAll(metricLoadSize.collect());

        return updateMetricsFromReport(mfs);
    }

    private boolean updateMetricsFromReport(List<Collector.MetricFamilySamples> mfs) {
        // Overall stats
        overall.update(currentReport.overallStats);
        overall.collect(mfs);

        // User stats
        for (FsImageReporter.UserStats userStat : currentReport.userStats.values()) {
            userFsMetrics.update(userStat, userStat.userName);
        }
        userFsMetrics.collect(mfs);

        // Group stats
        for (FsImageReporter.GroupStats groupStat : currentReport.groupStats.values()) {
            groupFsMetrics.update(groupStat, groupStat.groupName);
        }
        groupFsMetrics.collect(mfs);

        // Path stats
        if (currentReport.hasPathStats()) {
            for (FsImageReporter.PathStats pathStat : currentReport.pathStats.values()) {
                pathFsMetrics.update(pathStat, pathStat.path);
            }
            pathFsMetrics.collect(mfs);
        }

        // Path set stats
        if (currentReport.hasPathSetStats()) {
            for (FsImageReporter.PathStats pathStat : currentReport.pathSetStats.values()) {
                pathSetFsMetrics.update(pathStat, pathStat.path);
            }
            pathSetFsMetrics.collect(mfs);
        }

        currentReport.collect(mfs);

        // Signal error, if background thread ran into error
        return currentReport.error;
    }


    void onFsImageChange(File fsImageFile) {
        try {
            lock.lock();

            // Load new fsimage ...
            FSImageLoader loader = loadFsImage(fsImageFile);

            // ... compute stats
            try (Summary.Timer timer = metricVisitDuration.startTimer()) {
                report = FsImageReporter.computeStatsReport(loader, config);
            }
            reportUpdated.signalAll(); // Notify any waits
        } catch (Exception e) {
            LOGGER.error("Can not load FSImage", e);
            report.error = true;
        } finally {
            lock.unlock();
        }
    }

    private FSImageLoader loadFsImage(File fsImageFile) throws IOException {
        metricLoadSize.set(fsImageFile.length());

        try (RandomAccessFile raFile = new RandomAccessFile(fsImageFile, "r")) {
            long time = System.currentTimeMillis();
            try (Summary.Timer timer = metricLoadDuration.startTimer()) {
                final FSImageLoader loader = FSImageLoader.load(raFile);
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("Loaded {} with {}MiB in {}ms", fsImageFile.getAbsoluteFile(),
                            String.format("%.1f", fsImageFile.length() / 1024.0 / 1024.0),
                            System.currentTimeMillis() - time);
                }
                return loader;
            }
        }
    }

    /**
     * Gets current report.
     * <p>
     * Blocks if current report is still in computation.
     * @see #onFsImageChange(File)
     *
     * @return the current FSImage report.
     */
    FsImageReporter.Report getFsImageReport() {
        // Use current report if exists, otherwise wait
        if(null==report) {
            // Blocks till there is a computed report
            lock.lock();
            try {
                while (null == report) {
                    reportUpdated.awaitUninterruptibly();
                }
            } finally {
                lock.unlock();
            }
        }

        return report;
    }
}
