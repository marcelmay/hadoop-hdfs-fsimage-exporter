package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.File;
import java.io.Reader;
import java.util.concurrent.TimeUnit;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.ResponseBody;
import org.junit.Test;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.yaml.snakeyaml.Yaml;

/**
 * Benchmark and stress fsimage exporter
 */
public class BenchmarkIT {

    @State(Scope.Benchmark)
    public static class BenchmarkState {
        WebServer server;
        OkHttpClient client;
        Request request;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            // Configure and run exporter
            Config config;
            try (Reader reader = new InputStreamReader(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream("config-it.yml"))) {
                config = new Yaml().loadAs(reader, Config.class);
            }
            server = new WebServer().configure(config, "localhost", 7772);

            // Prepare request fetching
            client = new OkHttpClient();
            request = new Request.Builder()
                    .url("http://localhost:7772/metrics")
                    .build();
        }

        private String getResponse() throws IOException {
            final Response response = client.newCall(request).execute();
            final ResponseBody body = response.body();
            String bodyContent = body.toString();
            body.close();
            response.close();
            return bodyContent;
        }

        @TearDown
        public void tearDown() throws Exception {
            server.stop();
        }
    }

    @Benchmark
    public void pollExporter(Blackhole blackhole, BenchmarkState state) throws IOException {
        blackhole.consume(state.getResponse());
    }

    @Test
    public void runMicroBenchMark() throws RunnerException {
        new File("target/jmh-report/").mkdirs();
        Options opt = new OptionsBuilder()
                .include(getClass().getName())
                .warmupIterations(2)
                .measurementIterations(10)
                .mode(Mode.AverageTime)
                .timeUnit(TimeUnit.MILLISECONDS)
                .addProfiler(GCProfiler.class)
                .jvmArgs("-server", "-XX:+UseG1GC", "-Xmx256m")
                .shouldDoGC(true)
                .forks(1)
                .resultFormat(ResultFormatType.JSON)
                .result("target/jmh-report/"+getClass().getSimpleName()+".json")
                .build();

        new Runner(opt).run();
    }

}
