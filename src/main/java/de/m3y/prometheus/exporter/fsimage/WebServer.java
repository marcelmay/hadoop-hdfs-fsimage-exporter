package de.m3y.prometheus.exporter.fsimage;

import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Info;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import org.apache.log4j.Level;
import org.apache.log4j.spi.RootLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;

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
    private final Info buildInfo = Info.build()
            .name("fsimage_exporter_build")
            .help("Hadoop FSImage exporter build info")
            .labelNames("appVersion", "buildTime", "buildScmVersion", "buildScmBranch").create();


    WebServer configure(Config config, String address, int port) throws IOException {
        // Exporter own JVM metrics
        DefaultExports.initialize();

        // Build info
        buildInfo.labels(
                BuildMetaInfo.INSTANCE.getVersion(),
                BuildMetaInfo.INSTANCE.getBuildTimeStamp(),
                BuildMetaInfo.INSTANCE.getBuildScmVersion(),
                BuildMetaInfo.INSTANCE.getBuildScmBranch()
        );
        buildInfo.register();

        // Configure HTTP server
        InetSocketAddress inetAddress = new InetSocketAddress(address, port);
        httpServer = new HTTPServerWithCustomHandler(inetAddress);
        httpServer.replaceRootHandler(new ConfigHttpHandler(config));
        LOG.info("FSImage exporter started and listening on http://{}:{}", inetAddress.getHostName(), inetAddress.getPort());

        // Waits for parsed fsimage, so should run last after started HTTP server
        fsImageCollector = new FsImageCollector(config);
        fsImageCollector.register();

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
