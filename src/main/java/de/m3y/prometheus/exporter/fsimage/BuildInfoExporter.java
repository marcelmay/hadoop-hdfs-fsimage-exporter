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
    private static final List<String> LABEL_NAMES =
        Arrays.asList("appName", "appVersion", "buildTime", "buildScmVersion", "buildScmBranch");

    private final String appName;
    private final String metricName;

    /**
     * @param metricPrefix the metric name prefix
     * @param appName the application name to report this metric for.
     */
    public BuildInfoExporter(String metricPrefix, String appName) {
        super();
        this.appName = appName;
        metricName = metricPrefix + "app_info";
    }

    public String getAppName() {
        return appName;
    }

    public String getAppVersion() {
        return "<will be replaced>";
    }

    public String getBuildTimeStamp() {
        return "<will be replaced>";
    }

    public String getBuildScmVersion() {
        return "<will be replaced>";
    }

    public String getBuildScmBranch() {
        return "<will be replaced>";
    }

    @Override
    public List<MetricFamilySamples> collect() {
        GaugeMetricFamily metricFamily = new GaugeMetricFamily(metricName, "Application build info",
            LABEL_NAMES);
        metricFamily.addMetric(Arrays.asList(
            getAppName(),
            getAppVersion(),
            getBuildTimeStamp(),
            getBuildScmVersion(),
            getBuildScmBranch()
        ), 1.0D);
        return Collections.singletonList(metricFamily);
    }
}

