package de.m3y.prometheus.exporter.fsimage;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import org.yaml.snakeyaml.Yaml;

public class WebServer {

    private HTTPServer server;

    WebServer configure(Config config, String address, int port) throws IOException {
        // Metrics
        final FsImageCollector fsImageCollector = new FsImageCollector(config);
        fsImageCollector.register();

        new MemoryPoolsExports().register();

        final BuildInfoExporter buildInfo = new BuildInfoExporter("fsimage_exporter_",
                "fsimage_exporter").register();

        // Jetty
        InetSocketAddress inetAddress = new InetSocketAddress(address, port);
        server = new HTTPServer(address, port, true){
            @Override
            protected void configureContextHandlers(CollectorRegistry registry) {
                this.server.createContext("/", new ConfigHttpHandler(config, buildInfo));
                this.server.createContext("/metrics",  createMetricsHandler(registry));
            }

            @Override
            public void stop() {
                super.stop();
                fsImageCollector.shutdown();
            }
        };

        return this;
    }

    public void stop() {
        server.stop();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WebServer <hostname> <port> <yml configuration file>"); // NOSONAR
            System.exit(1);
        }

        Config config;
        try (FileReader reader = new FileReader(args[2])) {
            config = new Yaml().loadAs(reader, Config.class);
        }

        new WebServer().configure(config, args[0], Integer.parseInt(args[1]));
    }
}
