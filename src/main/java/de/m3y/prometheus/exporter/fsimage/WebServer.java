package de.m3y.prometheus.exporter.fsimage;

import java.io.FileReader;
import java.io.IOException;
import java.net.InetSocketAddress;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import org.yaml.snakeyaml.Yaml;

public class WebServer {

    static class HTTPServerWithCustomHandler extends HTTPServer {

        HTTPServerWithCustomHandler(InetSocketAddress addr) throws IOException {
            super(addr, CollectorRegistry.defaultRegistry, true);
        }

        void replaceRootHandler(ConfigHttpHandler configHttpHandler) {
            server.removeContext("/");
            server.createContext("/", configHttpHandler);
        }
    }

    private HTTPServerWithCustomHandler httpServer;
    private FsImageCollector fsImageCollector;

    WebServer configure(Config config, String address, int port) throws IOException {
        // Metrics
        fsImageCollector = new FsImageCollector(config);
        fsImageCollector.register();

        new MemoryPoolsExports().register();

        final BuildInfoExporter buildInfo = new BuildInfoExporter("fsimage_exporter_",
                "fsimage_exporter").register();

        InetSocketAddress inetAddress = new InetSocketAddress(address, port);
        httpServer = new HTTPServerWithCustomHandler(inetAddress);
        httpServer.replaceRootHandler(new ConfigHttpHandler(config, buildInfo));

        return this;
    }

    public void stop() {
        httpServer.stop();
        fsImageCollector.shutdown();
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
