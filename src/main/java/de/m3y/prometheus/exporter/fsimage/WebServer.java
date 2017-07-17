package de.m3y.prometheus.exporter.fsimage;

import java.io.FileReader;
import java.net.InetSocketAddress;

import io.prometheus.client.exporter.MetricsServlet;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.yaml.snakeyaml.Yaml;

public class WebServer {
    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WebServer <hostname> <port> <yml configuration file>");
            System.exit(1);
        }

        Config config;
        try (FileReader reader = new FileReader(args[2])) {
            config = new Yaml().loadAs(reader, Config.class);
        }

        new HfsaFsImageCollector(config).register();

        final BuildInfoExporter buildInfo = new BuildInfoExporter("fsimage_exporter_",
                "fsimage_exporter").register();

        int port = Integer.parseInt(args[1]);
        InetSocketAddress inetAddress = new InetSocketAddress(args[0], port);
        Server server = new Server(inetAddress);
        ServletContextHandler context = new ServletContextHandler();
        context.setContextPath("/");
        server.setHandler(context);
        context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
        context.addServlet(new ServletHolder(new HomePageServlet(config, buildInfo)), "/");
        server.start();
        server.join();
    }
}
