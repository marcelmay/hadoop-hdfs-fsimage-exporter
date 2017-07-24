package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class HomePageServlet extends HttpServlet {

    final private Config config;
    final private BuildInfoExporter buildInfoExporter;

    public HomePageServlet(Config config, BuildInfoExporter buildInfoExporter) {
        this.config = config;
        this.buildInfoExporter = buildInfoExporter;
    }

    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("text/html");
        resp.getWriter().print("<html>\n"
                + "<head><title>Hadoop HDFS FSImage Exporter</title></head>\n"
                + "<body>\n"
                + "<h1>Hadoop HDFS FSImage Exporter</h1>\n"
                + "<p><a href=\"/metrics\">Metrics</a></p>\n"
                + "<h2>Build info</h2>"
                + "<ul>"
                + "<li>App version: " + buildInfoExporter.getAppVersion() + "</li>"
                + "<li>Build time : " + buildInfoExporter.getBuildTimeStamp() + "</li>"
                + "<li>SCM branch : " + buildInfoExporter.getBuildScmBranch() + "</li>"
                + "<li>SCM version : " + buildInfoExporter.getBuildScmVersion() + "</li>"
                + "</ul>"
                + "<h2>Configuration</h2><ul>"
                + "<li>Path to HDFS NameNode fsImage snapshots : " + config.getFsImagePath() + "</li>"
                + "<li>Skip re-parsing previously parsed fsImage files : " + config.isSkipPreviouslyParsed() + "</li>"
                + "</ul></body>\n"
                + "</html>");
    }
}