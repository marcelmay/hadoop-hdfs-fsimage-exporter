package de.m3y.prometheus.exporter.fsimage;

import io.prometheus.client.Collector;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import static de.m3y.prometheus.assertj.MetricFamilySamplesAssert.assertThat;
import static de.m3y.prometheus.assertj.MetricFamilySamplesAssert.labelValues;
import static de.m3y.prometheus.assertj.MetricFamilySamplesUtils.getMetricFamilySamples;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class FsImageCollectorTest {
    @Test
    public void testCollect() {
        Config config = new Config();
        config.setFsImagePath("src/test/resources");
        FsImageCollector fsImageCollector = new FsImageCollector(config);
        final List<Collector.MetricFamilySamples> metricFamilySamples = fsImageCollector.collect();

        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_scrape_requests"))
                .hasTypeOfCounter()
                .hasSampleValue(1.0);
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_scrape_requests")).hasTypeOfCounter()
                .hasSampleValue(1);
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_compute_stats_duration_seconds"))
                .hasTypeOfSummary()
                .hasSampleCountValue(1)
                .hasSampleSumValue(da -> da.isGreaterThan(0).isLessThan(1));
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_scrape_duration_seconds")).hasTypeOfGauge()
                .hasSampleValue(da -> da.isGreaterThan(0).isLessThan(1));
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_load_file_size_bytes")).hasTypeOfGauge()
                .hasSampleValue(2420);
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_scrape_errors")).hasTypeOfCounter()
                .hasSampleValue(0.0);
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_load_duration_seconds"))
                .hasTypeOfSummary()
                .hasSampleSumValue(da -> da.isGreaterThan(0).isLessThan(1))
                .hasSampleCountValue(1.0);
    }

    @Test
    public void testCollectNonExistingPath() {
        Config config = new Config();
        config.setFsImagePath("src/test/resources");
        config.setPaths(new HashSet<>(Collections.singletonList("/non/existing/path/.*")));
        FsImageCollector fsImageCollector = new FsImageCollector(config);
        final List<Collector.MetricFamilySamples> metricFamilySamples = fsImageCollector.collect();

        // Check no path metrics exist
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getMetricFamilySamples(metricFamilySamples, "fsimage_path_dirs"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getMetricFamilySamples(metricFamilySamples, "fsimage_path_links"));
        assertThatExceptionOfType(IllegalArgumentException.class)
                .isThrownBy(() -> getMetricFamilySamples(metricFamilySamples, "fsimage_path_fsize"));
    }

    @Test
    public void testCollectForPath() {
        Config config = new Config();
        config.setFsImagePath("src/test/resources");
        config.setPaths(new HashSet<>(Collections.singletonList("/datalake/.*")));
        FsImageCollector fsImageCollector = new FsImageCollector(config);
        final List<Collector.MetricFamilySamples> metricFamilySamples = fsImageCollector.collect();

        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_path_dirs"))
                .hasTypeOfGauge()
                .hasSampleLabelNames("path")
                .hasSampleValue(labelValues("/datalake/asset1"), 0.0)
                .hasSampleValue(labelValues("/datalake/asset2"), 0.0)
                .hasSampleValue(labelValues("/datalake/asset3"), 2.0);
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_path_links"))
                .hasTypeOfGauge()
                .hasSampleValue(labelValues("/datalake/asset1"), 0.0)
                .hasSampleValue(labelValues("/datalake/asset2"), 0.0)
                .hasSampleValue(labelValues("/datalake/asset3"), 0.0);
        assertThat(getMetricFamilySamples(metricFamilySamples, "fsimage_path_fsize"))
                .hasTypeOfHistogram()
                // 1st asset
                .hasSampleCountValue(labelValues("/datalake/asset1"), 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), 0.0, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), 1048576, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), 3.3554432E7, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), 6.7108864E7, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), 1.34217728E8, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), 1.073741824E9, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), 1.073741824E10, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset1"), Double.POSITIVE_INFINITY, 0.0)
                // 2nd asset
                .hasSampleCountValue(labelValues("/datalake/asset2"), 2.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), 0.0, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), 1048576, 1.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), 3.3554432E7, 2.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), 6.7108864E7, 2.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), 1.34217728E8, 2.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), 1.073741824E9, 2.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), 1.073741824E10, 2.0)
                .hasSampleBucketValue(labelValues("/datalake/asset2"), Double.POSITIVE_INFINITY, 2.0)
                // 3rd asset
                .hasSampleCountValue(labelValues("/datalake/asset3"), 3.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), 0.0, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), 1048576, 0.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), 3.3554432E7, 3.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), 6.7108864E7, 3.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), 1.34217728E8, 3.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), 1.073741824E9, 3.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), 1.073741824E10, 3.0)
                .hasSampleBucketValue(labelValues("/datalake/asset3"), Double.POSITIVE_INFINITY, 3.0)
        ;
    }
}
