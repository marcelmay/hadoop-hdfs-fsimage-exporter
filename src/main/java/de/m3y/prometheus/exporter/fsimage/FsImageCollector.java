package de.m3y.prometheus.exporter.fsimage;

import io.prometheus.client.Collector;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Collects stats from Hadoop FSImage.
 * <p>
 * Note:
 * <ul>
 * <li>A background thread watches and parses FSImage, therefore not blocking metrics collection itself.
 * Parse time depends on FSImage size and can be up to minutes.
 * <p>
 * See {@link FsImageWatcher}
 * </li>
 * </ul>
 */
public class FsImageCollector extends Collector {
    private static final Logger LOGGER = LoggerFactory.getLogger(FsImageCollector.class);

    static final String METRIC_PREFIX = "fsimage_";

    private final Counter scapeRequests = Counter.build()
            .name(METRIC_PREFIX + "scrape_requests_total")
            .help("Exporter requests made").create();
    private final Counter scrapeErrors = Counter.build()
            .name(METRIC_PREFIX + "scrape_errors_total")
            .help("Counts failed scrapes.").create();
    private final Gauge scrapeDuration = Gauge.build()
            .name(METRIC_PREFIX + "scrape_duration_seconds")
            .help("Scrape duration").create();

    private final FsImageUpdateHandler fsImageReportUpdater;


    private final ScheduledExecutorService scheduler;

    FsImageCollector(Config config) {
        final String path = config.getFsImagePath();
        if (null == path || path.isEmpty()) {
            throw new IllegalArgumentException("Please set the the directory location to the FSImage snapshots (fsImagePath)");
        }

        File fsImageDir = new File(path);
        if (!fsImageDir.exists()) {
            throw new IllegalArgumentException("The directory for FSImage snapshots (fsImagePath) " +
                    fsImageDir.getAbsolutePath() + " does not exist");
        }

        fsImageReportUpdater = new FsImageUpdateHandler(config);
        FsImageWatcher fsImageWatcher = new FsImageWatcher(fsImageDir,
                fsImageReportUpdater::onFsImageChange);

        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleWithFixedDelay(fsImageWatcher, 0 /* Trigger immediately */, 60, TimeUnit.SECONDS);
    }

    @Override
    public List<MetricFamilySamples> collect() {
        List<MetricFamilySamples> mfs = new ArrayList<>();

        try (Gauge.Timer timer = scrapeDuration.startTimer()) {
            scapeRequests.inc();

            if (fsImageReportUpdater.collectFsImageSamples(mfs)) {
                scrapeErrors.inc();
            }
        } catch (Exception e) {
            scrapeErrors.inc();
            LOGGER.error("FSImage scrape failed", e);
        }

        mfs.addAll(scrapeDuration.collect());
        mfs.addAll(scapeRequests.collect());
        mfs.addAll(scrapeErrors.collect());

        return mfs;
    }

    /**
     * Closes resources such as scheduler for background parsing thread.
     */
    public void shutdown() {
        scheduler.shutdown();
    }

}

