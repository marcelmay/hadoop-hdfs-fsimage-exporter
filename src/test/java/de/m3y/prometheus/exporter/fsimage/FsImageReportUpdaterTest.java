package de.m3y.prometheus.exporter.fsimage;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import io.prometheus.client.Collector;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FsImageReportUpdaterTest {

    @Test(timeout = 1000L)
    public void testGetReportWhenFileChanges() throws ExecutionException, InterruptedException {
        Config config = new Config();
        FsImageUpdateHandler fsImageReportUpdater = new FsImageUpdateHandler(config);
        final Future<FsImageReporter.Report> submit = Executors.newSingleThreadExecutor()
                .submit(fsImageReportUpdater::getFsImageReport);
        assertThat(submit.isDone()).isFalse();

        // Trigger report generation
        final File fsImageFile = new File("src/test/resources/fsimage_0001");
        fsImageReportUpdater.onFsImageChange(fsImageFile);

        // Verify result
        final FsImageReporter.Report report = submit.get();
        assertThat(report.error).isFalse();

        List<Collector.MetricFamilySamples> mfs = new ArrayList<>();
        fsImageReportUpdater.collectFsImageSamples(mfs);
        assertThat(mfs).hasSize(17);
    }
}
