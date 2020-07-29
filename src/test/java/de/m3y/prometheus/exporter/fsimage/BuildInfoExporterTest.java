package de.m3y.prometheus.exporter.fsimage;

import java.util.Arrays;
import java.util.List;

import io.prometheus.client.Collector;
import org.junit.Test;

import static de.m3y.prometheus.assertj.MetricFamilySamplesAssert.assertThat;
import static de.m3y.prometheus.assertj.MetricFamilySamplesUtils.getMetricFamilySamples;

public class BuildInfoExporterTest {
    @Test
    public void testCollect() {
        BuildInfoExporter buildInfoExporter = new BuildInfoExporter("my_fsimage_", "foo");
        final List<Collector.MetricFamilySamples> collect = buildInfoExporter.collect();
        assertThat(getMetricFamilySamples(collect, "my_fsimage_build_info"))
                .hasTypeOfGauge()
                .hasSampleLabelNames("appName", "appVersion", "buildTime", "buildScmVersion", "buildScmBranch")
                .hasSampleValue(Arrays.asList(
                        buildInfoExporter.getAppName(),
                        buildInfoExporter.getAppVersion(),
                        buildInfoExporter.getBuildTimeStamp(),
                        buildInfoExporter.getBuildScmVersion(),
                        buildInfoExporter.getBuildScmBranch()
                ), 1);
    }
}
