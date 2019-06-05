package de.m3y.prometheus.exporter.fsimage;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import io.prometheus.client.Collector;
import io.prometheus.client.GaugeMetricFamily;

/**
 * Exports build info such as version, build time etc.
 */
public class BuildInfoExporter extends Collector {
    private final String appName;
    private final GaugeMetricFamily metricFamily;

    /**
     * @param metricPrefix the metric name prefix
     * @param appName      the application name to report this metric for.
     */
    public BuildInfoExporter(String metricPrefix, String appName) {
        super();
        this.appName = appName;

        metricFamily = new GaugeMetricFamily(metricPrefix + "app_info",
                "Application build info",
                Arrays.asList("appName", "appVersion", "buildTime", "buildScmVersion", "buildScmBranch"));
        metricFamily.addMetric(
                Arrays.asList(
                        getAppName(),
                        getAppVersion(),
                        getBuildTimeStamp(),
                        getBuildScmVersion(),
                        getBuildScmBranch()
                ), 1.0D);
    }

    public String getAppName() {
        return appName;
    }

    public String getAppVersion() {
        return "<will be replaced>"; // NOSONAR
    }

    public String getBuildTimeStamp() {
        return "<will be replaced>"; // NOSONAR
    }

    public String getBuildScmVersion() {
        return "<will be replaced>"; // NOSONAR
    }

    public String getBuildScmBranch() {
        return "<will be replaced>"; // NOSONAR
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return Collections.singletonList(metricFamily);
    }
}

