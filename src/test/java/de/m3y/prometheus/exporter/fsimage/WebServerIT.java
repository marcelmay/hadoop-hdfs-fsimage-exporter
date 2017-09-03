package de.m3y.prometheus.exporter.fsimage;

import java.io.FileReader;
import java.io.IOException;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.eclipse.jetty.server.Server;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class WebServerIT {
    private Server server;
    private String exporterBaseUrl;
    private OkHttpClient client;

    @Before
    public void setUp() throws Exception {
        Config config;
        try (FileReader reader = new FileReader("example.yml")) {
            config = new Yaml().loadAs(reader, Config.class);
        }

        server = new WebServer().configure(config, "localhost", 7772).start();
        exporterBaseUrl = "http://localhost:7772";
        client = new OkHttpClient();
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    @Test
    public void testMetrics() throws Exception {
        Response response = getResponse(exporterBaseUrl + "/metrics");
        assertEquals(200, response.code());
        String body = response.body().string();

        // App info
        assertTrue(body.contains("fsimage_exporter_app_info{appName=\"fsimage_exporter\",appVersion=\""));
        assertThat(body, containsString("fsimage_scrape_requests_total "));
        assertTrue(body.contains("fsimage_scrape_skips_total 0.0"));
        assertTrue(body.contains("fsimage_compute_stats_duration_seconds_count "));
        assertTrue(body.contains("fsimage_compute_stats_duration_seconds_sum "));
        assertTrue(body.contains("fsimage_scrape_duration_seconds "));
        assertTrue(body.contains("fsimage_load_file_size_bytes 2420.0"));
        assertTrue(body.contains("fsimage_scrape_errors_total 0.0"));
        assertTrue(body.contains("fsimage_load_duration_seconds_count 1.0"));
        assertTrue(body.contains("fsimage_load_duration_seconds_sum "));

        // JVM GC Info
        assertTrue(body.contains("jvm_memory_pool_bytes_used{"));
        assertTrue(body.contains("jvm_memory_bytes_used{"));

        // Overall
        assertTrue(body.contains("fsimage_dirs 14.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"1048576.0\",} 4.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"3.3554432E7\",} 13.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"6.7108864E7\",} 14.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"1.34217728E8\",} 15.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"1.073741824E9\",} 16.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"1.073741824E10\",} 16.0"));
        assertTrue(body.contains("fsimage_fsize_bucket{le=\"+Inf\",} 16.0"));
        assertTrue(body.contains("fsimage_fsize_count 16.0"));
        assertTrue(body.contains("fsimage_fsize_sum 3.56409344E8"));
        assertTrue(body.contains("fsimage_blocks 17.0"));
        assertTrue(body.contains("fsimage_links 0.0"));

        // Group
        assertTrue(body.contains("fsimage_group_links{group_name=\"root\",} 0.0"));
        assertTrue(body.contains("fsimage_group_links{group_name=\"supergroup\",} 0.0"));
        assertTrue(body.contains("fsimage_group_links{group_name=\"nobody\",} 0.0"));
        assertTrue(body.contains("fsimage_group_blocks{group_name=\"root\",} 1.0"));
        assertTrue(body.contains("fsimage_group_blocks{group_name=\"supergroup\",} 13.0"));
        assertTrue(body.contains("fsimage_group_blocks{group_name=\"nobody\",} 3.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"root\",le=\"1048576.0\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"root\",le=\"3.3554432E7\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"root\",le=\"6.7108864E7\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"root\",le=\"1.34217728E8\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"root\",le=\"1.073741824E9\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"root\",le=\"1.073741824E10\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"root\",le=\"+Inf\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_count{group_name=\"root\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_sum{group_name=\"root\",} 1024.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"1048576.0\",} 3.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"3.3554432E7\",} 11.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"6.7108864E7\",} 12.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"1.34217728E8\",} 13.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"1.073741824E9\",} 13.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"1.073741824E10\",} 13.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"supergroup\",le=\"+Inf\",} 13.0"));
        assertTrue(body.contains("fsimage_group_fsize_count{group_name=\"supergroup\",} 13.0"));
        assertTrue(body.contains("fsimage_group_fsize_sum{group_name=\"supergroup\",} 1.6766464E8"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"1048576.0\",} 0.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"3.3554432E7\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"6.7108864E7\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"1.34217728E8\",} 1.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"1.073741824E9\",} 2.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"1.073741824E10\",} 2.0"));
        assertTrue(body.contains("fsimage_group_fsize_bucket{group_name=\"nobody\",le=\"+Inf\",} 2.0"));
        assertTrue(body.contains("fsimage_group_fsize_count{group_name=\"nobody\",} 2.0"));
        assertTrue(body.contains("fsimage_group_fsize_sum{group_name=\"nobody\",} 1.8874368E8"));
        assertTrue(body.contains("fsimage_group_dirs{group_name=\"root\",} 0.0"));
        assertTrue(body.contains("fsimage_group_dirs{group_name=\"supergroup\",} 14.0"));
        assertTrue(body.contains("fsimage_group_dirs{group_name=\"nobody\",} 0.0"));


        // User
        assertTrue(body.contains("fsimage_user_blocks{user_name=\"foo\",} 2.0"));
        assertTrue(body.contains("fsimage_user_blocks{user_name=\"root\",} 1.0"));
        assertTrue(body.contains("fsimage_user_blocks{user_name=\"mm\",} 14.0"));
        assertTrue(body.contains("fsimage_user_links{user_name=\"foo\",} 0.0"));
        assertTrue(body.contains("fsimage_user_links{user_name=\"root\",} 0.0"));
        assertTrue(body.contains("fsimage_user_links{user_name=\"mm\",} 0.0"));
        assertTrue(body.contains("fsimage_user_dirs{user_name=\"foo\",} 0.0"));
        assertTrue(body.contains("fsimage_user_dirs{user_name=\"root\",} 0.0"));
        assertTrue(body.contains("fsimage_user_dirs{user_name=\"mm\",} 14.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"1048576.0\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"3.3554432E7\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"6.7108864E7\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"1.34217728E8\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"1.073741824E9\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"1.073741824E10\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"root\",le=\"+Inf\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_count{user_name=\"root\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_sum{user_name=\"root\",} 1024.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"1048576.0\",} 0.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"3.3554432E7\",} 0.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"6.7108864E7\",} 0.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"1.34217728E8\",} 0.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"1.073741824E9\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"1.073741824E10\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"foo\",le=\"+Inf\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_count{user_name=\"foo\",} 1.0"));
        assertTrue(body.contains("fsimage_user_fsize_sum{user_name=\"foo\",} 1.6777216E8"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"1048576.0\",} 3.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"3.3554432E7\",} 12.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"6.7108864E7\",} 13.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"1.34217728E8\",} 14.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"1.073741824E9\",} 14.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"1.073741824E10\",} 14.0"));
        assertTrue(body.contains("fsimage_user_fsize_bucket{user_name=\"mm\",le=\"+Inf\",} 14.0"));
        assertTrue(body.contains("fsimage_user_fsize_count{user_name=\"mm\",} 14.0"));
        assertTrue(body.contains("fsimage_user_fsize_sum{user_name=\"mm\",} 1.8863616E8"));


        // Paths
        assertTrue(body.contains("fsimage_path_blocks{path=\"/datalake/asset2\",} 2.0"));
        assertTrue(body.contains("fsimage_path_blocks{path=\"/datalake/asset3\",} 3.0"));
        assertTrue(body.contains("fsimage_path_blocks{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_blocks{path=\"/datalake/asset1\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"1048576.0\",} 1.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"3.3554432E7\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"6.7108864E7\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"1.34217728E8\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"1.073741824E9\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"1.073741824E10\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset2\",le=\"+Inf\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/datalake/asset2\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/datalake/asset2\",} 2098176.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"1048576.0\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"3.3554432E7\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"6.7108864E7\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"1.34217728E8\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"1.073741824E9\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"1.073741824E10\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset3\",le=\"+Inf\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/datalake/asset3\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/datalake/asset3\",} 6291456.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"1048576.0\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"3.3554432E7\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"6.7108864E7\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"1.34217728E8\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"1.073741824E9\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"1.073741824E10\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/user/mm\",le=\"+Inf\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"0.0\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"1048576.0\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"3.3554432E7\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"6.7108864E7\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"1.34217728E8\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"1.073741824E9\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"1.073741824E10\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_bucket{path=\"/datalake/asset1\",le=\"+Inf\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/datalake/asset1\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/datalake/asset1\",} 0.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/datalake/asset2\",} 1.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/datalake/asset3\",} 3.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/user/mm\",} 1.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/datalake/asset1\",} 1.0"));

        assertTrue(body.contains("fsimage_path_links{path=\"/datalake/asset2\",} 0.0"));
        assertTrue(body.contains("fsimage_path_links{path=\"/datalake/asset3\",} 0.0"));
        assertTrue(body.contains("fsimage_path_links{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_links{path=\"/datalake/asset1\",} 0.0"));

        // Test welcome page
        response = getResponse(exporterBaseUrl);
        assertEquals(200, response.code());
        body = response.body().string();
        assertTrue(body.contains("Hadoop HDFS FSImage Exporter"));
        assertTrue(body.contains("SCM branch"));
        assertTrue(body.contains("SCM version"));
        assertTrue(body.contains("Metrics"));
    }

    private Response getResponse(String url) throws IOException {
        Request request = new Request.Builder()
                .url(url)
                .build();
        return client.newCall(request).execute();
    }
}
