package de.m3y.prometheus.exporter.fsimage;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.MemoryPoolsExports;
import org.apache.log4j.Level;
import org.apache.log4j.spi.RootLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

public class WebServer {
    private static final Logger LOG = LoggerFactory.getLogger(WebServer.class);

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
        LOG.info("FSImage exporter started and listening on {}", inetAddress);

        return this;
    }

    public void stop() {
        httpServer.stop();
        fsImageCollector.shutdown();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: WebServer [-Dlog.level=[WARN|INFO|DEBUG]] <hostname> <port> <yml configuration file>"); // NOSONAR
            System.exit(1);
        }

        RootLogger.getRootLogger().setLevel(Level.toLevel(System.getProperty("log.level"), Level.INFO));

        try (FileInputStream reader = new FileInputStream(args[2])) {
            Config config = new Yaml().loadAs(reader, Config.class);
            new WebServer().configure(config, args[0], Integer.parseInt(args[1]));
        }
    }
}
