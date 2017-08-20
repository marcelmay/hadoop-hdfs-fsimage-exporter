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
    private static final Gauge METRIC_LOAD_SIZE = Gauge.build()
            .name(METRIC_PREFIX + "load_file_size_bytes")
            .help("Size of raw FSImage").register();
    private static final Counter METRIC_SCRAPE_SKIPS = Counter.build()
            .name(METRIC_PREFIX + "scrape_skips_total")
            .help("Counts the fsimage scrape skips (no fsimage change).").register();
    private static final Logger LOGGER = LoggerFactory.getLogger(FsImageWatcher.class);
    private final File fsImageDir;
    private final boolean skipPreviouslyParsed;
    private volatile FSImageLoader loader;
    private File lastFsImageFileLoaded;
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
    static final Comparator<File> FSIMAGE_FILENAME_COMPARATOR = new Comparator<File>() {
        @Override
        public int compare(File o1, File o2) {
            return o2.getName().compareTo(o1.getName());
        }
    };

    public FsImageWatcher(File fsImageDir, boolean skipPreviouslyParsed) {
        this.fsImageDir = fsImageDir;
        if (!fsImageDir.exists()) {
            throw new IllegalArgumentException(fsImageDir.getAbsolutePath() + " does not exist");
        }
        this.skipPreviouslyParsed = skipPreviouslyParsed;
    }

    @Override
    public void run() {
        try {
            load();
        } catch (IOException e) {
            LOGGER.error("Can not preload FSImage", e);
        }
    }

    void load() throws IOException {
        synchronized (lock) {
            File fsImageFile = findLatestFSImageFile(fsImageDir);
            if (skipPreviouslyParsed && fsImageFile.equals(lastFsImageFileLoaded)) {
                METRIC_SCRAPE_SKIPS.inc();
                LOGGER.debug("Skipping previously parsed {}", fsImageFile.getAbsoluteFile());
                return;
            }
            METRIC_LOAD_SIZE.set(fsImageFile.length());
            try (RandomAccessFile raFile = new RandomAccessFile(fsImageFile, "r")) {
                long time = System.currentTimeMillis();
                try (Summary.Timer timer = METRIC_LOAD_DURATION.startTimer()) {
                    loader = FSImageLoader.load(raFile);
                }
                LOGGER.info("Loaded {} in {}ms", fsImageDir.getAbsoluteFile(), System.currentTimeMillis() - time);
            }
            lastFsImageFileLoaded = fsImageFile;
        }
    }

    public FSImageLoader getFSImageLoader() {
        FSImageLoader currentLoader = loader;
        if (currentLoader == null) {
            synchronized (lock) {
                currentLoader = loader;
                if (currentLoader == null) {
                    run();
                    currentLoader = loader;
                }
            }
        }
        return currentLoader;
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
