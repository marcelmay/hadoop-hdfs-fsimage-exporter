package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Displays a welcome page containing build info and link to metrics.
 */
public class HomePageServlet extends HttpServlet {

    private final Config config;
    private final BuildInfoExporter buildInfoExporter;

    public HomePageServlet(Config config, BuildInfoExporter buildInfoExporter) {
        this.config = config;
        this.buildInfoExporter = buildInfoExporter;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        StringBuilder buf = new StringBuilder().append("<html>\n"
                + "<head><title>Hadoop HDFS FSImage Exporter</title></head>\n"
                + "<body>\n"
                + "<h1>Hadoop HDFS FSImage Exporter</h1>\n"
                + "<p><a href=\"/metrics\">Metrics</a></p>\n"
                + "<h2>Build info</h2>"
                + "<ul>"
                + "<li>App version: ").append(buildInfoExporter.getAppVersion()).append("</li>"
                + "<li>Build time : ").append(buildInfoExporter.getBuildTimeStamp()).append("</li>"
                + "<li>SCM branch : ").append(buildInfoExporter.getBuildScmBranch()).append("</li>"
                + "<li>SCM version : ").append(buildInfoExporter.getBuildScmVersion()).append("</li>"
                + "</ul>"
                + "<h2>Configuration</h2><ul>"
                + "<li>Path to HDFS NameNode fsImage snapshots : ").append(config.getFsImagePath()).append("</li>"
                + "<li>Skip re-parsing previously parsed fsImage files : ").append(config.isSkipPreviouslyParsed()).append("</li>");
        buf.append("<li>skipFileDistributionForGroupStats : ").append(config.isSkipFileDistributionForGroupStats()).append("</li>");
        buf.append("<li>skipFileDistributionForUserStats : ").append(config.isSkipFileDistributionForUserStats()).append("</li>");
        buf.append("<li>fileSizeDistributionBuckets : ").append(config.getFileSizeDistributionBuckets()).append("</li>");

        if (config.hasPaths()) {
            buf.append("<li>Paths : <ul>");
            appendAsListItems(buf, config.getPaths());
            buf.append("</ul></li>");
        }
        buf.append("<li>skipFileDistributionForPathStats : ").append(config.isSkipFileDistributionForPathStats()).append("</li>");

        if (config.hasPathSets()) {
            buf.append("<li>Path sets : <ul>");
            for (Map.Entry<String, List<String>> entry : config.getPathSets().entrySet()) {
                buf.append("<li>").append(entry.getKey()).append("<ul>");
                appendAsListItems(buf, entry.getValue());
                buf.append("</ul></li>");
            }
            buf.append("</ul></li>");
        }
        buf.append("<li>skipFileDistributionForPathSetStats : ").append(config.isSkipFileDistributionForPathSetStats()).append("</li>");
        buf.append(
                "</ul></body>\n"
                        + "</html>");
        resp.setContentType("text/html");
        resp.getWriter().print(buf);
    }

    private void appendAsListItems(StringBuilder buf, Collection<String> items) {
        for (String path : items) {
            buf.append("<li>").append(path).append("</li>");
        }
    }
}