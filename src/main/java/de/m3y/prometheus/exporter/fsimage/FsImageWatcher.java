package de.m3y.prometheus.exporter.fsimage;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Comparator;
import java.util.regex.Pattern;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Summary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static de.m3y.prometheus.exporter.fsimage.HfsaFsImageCollector.METRIC_PREFIX;

/**
 * Watches and (pre-)loads FSImage files.
 */
public class FsImageWatcher implements Runnable {
    private static final Summary METRIC_LOAD_DURATION = Summary.build()
            .name(METRIC_PREFIX + "load_duration_seconds")
            .help("Time for loading/parsing FSImage").register();
    private static final Summary METRIC_VISIT_DURATION = Summary.build()
            .name(METRIC_PREFIX + "compute_stats_duration_seconds")
            .help("Time for computing stats for a loaded FSImage").register();
    private static final Gauge METRIC_LOAD_SIZE = Gauge.build()
            .name(METRIC_PREFIX + "load_file_size_bytes")
            .help("Size of raw FSImage").register();
    private static final Counter METRIC_SCRAPE_SKIPS = Counter.build()
            .name(METRIC_PREFIX + "scrape_skips_total")
            .help("Counts the fsimage scrape skips (no fsimage change).").register();
    private static final Logger LOGGER = LoggerFactory.getLogger(FsImageWatcher.class);
    private final File fsImageDir;
    private final boolean skipPreviouslyParsed;
    private File lastFsImageFileLoaded;
    private volatile FsImageReporter.Report report;
    private final Object lock = new Object();


    /**
     * Filters fsimage names.
     */
    static class FSImageFilenameFilter implements FilenameFilter {
        static final Pattern FS_IMAGE_PATTERN = Pattern.compile("fsimage_\\d+");

        @Override
        public boolean accept(File dir, String name) {
            return FS_IMAGE_PATTERN.matcher(name).matches();
        }
    }

    static final FilenameFilter FSIMAGE_FILTER = new FSImageFilenameFilter();

    /**
     * Sorts descending by file name.
     */
    static final Comparator<File> FSIMAGE_FILENAME_COMPARATOR = (o1, o2) -> o2.getName().compareTo(o1.getName());

    public FsImageWatcher(File fsImageDir, boolean skipPreviouslyParsed) {
        this.fsImageDir = fsImageDir;
        if (!fsImageDir.exists()) {
            throw new IllegalArgumentException(fsImageDir.getAbsolutePath() + " does not exist");
        }
        this.skipPreviouslyParsed = skipPreviouslyParsed;
    }

    @Override
    public void run() {
        synchronized (lock) {
            try {
                File fsImageFile = findLatestFSImageFile(fsImageDir);
                if (skipPreviouslyParsed && fsImageFile.equals(lastFsImageFileLoaded)) {
                    METRIC_SCRAPE_SKIPS.inc();
                    LOGGER.debug("Skipping previously parsed {}", fsImageFile.getAbsoluteFile());
                    return;
                }

                // Load new fsimage ...
                METRIC_LOAD_SIZE.set(fsImageFile.length());
                FSImageLoader loader;
                try (RandomAccessFile raFile = new RandomAccessFile(fsImageFile, "r")) {
                    long time = System.currentTimeMillis();
                    try (Summary.Timer timer = METRIC_LOAD_DURATION.startTimer()) {
                        loader = FSImageLoader.load(raFile);
                    }
                    LOGGER.info("Loaded {} in {}ms", fsImageFile.getAbsoluteFile(), System.currentTimeMillis() - time);
                }
                lastFsImageFileLoaded = fsImageFile;

                // ... compute stats
                try (Summary.Timer timer = METRIC_VISIT_DURATION.startTimer()) {
                    report = FsImageReporter.computeStatsReport(loader);
                }
            } catch (Exception e) {
                LOGGER.error("Can not preload FSImage", e);
                report.error = true;
            }
        }
    }

    public FsImageReporter.Report getFsImageReport() {
        FsImageReporter.Report currentReport = report;
        if (currentReport == null) {
            synchronized (lock) {
                currentReport = report;
                if (currentReport == null) {
                    run();
                    currentReport = report;
                }
            }
        }
        return currentReport;
    }

    static File findLatestFSImageFile(File fsImageDir) {
        // Check dir
        if (!fsImageDir.exists()) {
            throw new IllegalArgumentException(fsImageDir.getAbsolutePath() + " doest not exist");
        }

        final File[] files = fsImageDir.listFiles(FSIMAGE_FILTER);
        if (null == files || files.length == 0) {
            throw new IllegalStateException("No fsimage files found in " + fsImageDir.getAbsolutePath()
                    + " matching " + FsImageWatcher.FSImageFilenameFilter.FS_IMAGE_PATTERN);
        }

        Arrays.sort(files, FSIMAGE_FILENAME_COMPARATOR);

        return files[0];  // Youngest fsimage
    }
}
