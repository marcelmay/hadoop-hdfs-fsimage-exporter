package de.m3y.prometheus.exporter.fsimage;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.sun.net.httpserver.HttpExchange;

/**
 * Displays a welcome page containing build info and link to metrics.
 */
public class ConfigHttpHandler implements com.sun.net.httpserver.HttpHandler {

    private final Config config;
    private final BuildInfoExporter buildInfoExporter;

    public ConfigHttpHandler(Config config, BuildInfoExporter buildInfoExporter) {
        this.config = config;
        this.buildInfoExporter = buildInfoExporter;
    }

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        byte[] bytes = buildContent().getBytes(StandardCharsets.UTF_8);
        os.write(bytes, 0, bytes.length);

        final int contentSize = os.size();
        httpExchange.getResponseHeaders().set("Content-Type", "text/html");
        httpExchange.getResponseHeaders().set("Content-Length", String.valueOf(contentSize));
        httpExchange.sendResponseHeaders(HttpURLConnection.HTTP_OK, contentSize);
        os.writeTo(httpExchange.getResponseBody());
        httpExchange.close();
    }

    private String buildContent() {
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
                + "<li>Path to HDFS NameNode fsImage snapshots : ").append(config.getFsImagePath()).append("</li>");
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
        return buf.toString();
    }

    private void appendAsListItems(StringBuilder buf, Collection<String> items) {
        for (String path : items) {
            buf.append("<li>").append(path).append("</li>");
        }
    }
}