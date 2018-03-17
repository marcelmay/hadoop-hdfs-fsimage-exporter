package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * Test fsimage data:
 *
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 09:58 /datalake
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 09:57 /datalake/asset1
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset2
 * -rw-r--r--   1 mm   supergroup       1024 2017-07-22 10:00 /datalake/asset2/test_1KiB.img
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset2/test_2MiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset3
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset3/subasset1
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset3/subasset1/test_2MiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-07-22 10:01 /datalake/asset3/subasset2
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset3/subasset2/test_2MiB.img
 * -rw-r--r--   1 mm   supergroup    2097152 2017-07-22 10:01 /datalake/asset3/test_2MiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:03 /test1
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:03 /test2
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:25 /test3
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:11 /test3/foo
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:25 /test3/foo/bar
 * -rw-r--r--   1 mm   nobody       20971520 2017-06-17 23:13 /test3/foo/bar/test_20MiB.img
 * -rw-r--r--   1 mm   supergroup    2097152 2017-06-17 23:10 /test3/foo/bar/test_2MiB.img
 * -rw-r--r--   1 mm   supergroup   41943040 2017-06-17 23:25 /test3/foo/bar/test_40MiB.img
 * -rw-r--r--   1 mm   supergroup    4145152 2017-06-17 23:10 /test3/foo/bar/test_4MiB.img
 * -rw-r--r--   1 mm   supergroup    5181440 2017-06-17 23:10 /test3/foo/bar/test_5MiB.img
 * -rw-r--r--   1 mm   supergroup   83886080 2017-06-17 23:25 /test3/foo/bar/test_80MiB.img
 * -rw-r--r--   1 root root             1024 2017-06-17 23:09 /test3/foo/test_1KiB.img
 * -rw-r--r--   1 mm   supergroup   20971520 2017-06-17 23:11 /test3/foo/test_20MiB.img
 * -rw-r--r--   1 mm   supergroup    1048576 2017-06-17 23:07 /test3/test.img
 * -rw-r--r--   1 foo  nobody      167772160 2017-06-17 23:25 /test3/test_160MiB.img
 * -rw-r--r--   1 mm   supergroup       2048 2017-07-08 08:00 /test_2KiB.img
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:04 /user
 * drwxr-xr-x   - mm   supergroup          0 2017-06-17 23:04 /user/mm
 */
public class WebServerIT {
    private  WebServer server;
    private String exporterBaseUrl;
    private OkHttpClient client;

    @Before
    public void setUp() throws Exception {
        Config config;
        try (Reader reader = new InputStreamReader(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("config-it.yml"))) {
            config = new Yaml().loadAs(reader, Config.class);
        }

        server = new WebServer().configure(config, "localhost", 7772);
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
        assertTrue(body.contains("fsimage_replication_count 16.0"));
        assertTrue(body.contains("fsimage_replication_sum 22.0"));

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
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/datalake/asset2\",} 2.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/datalake/asset2\",} 2098176.0"));
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/datalake/asset3\",} 3.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/datalake/asset3\",} 6291456.0"));
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_count{path=\"/datalake/asset1\",} 0.0"));
        assertTrue(body.contains("fsimage_path_fsize_sum{path=\"/datalake/asset1\",} 0.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/datalake/asset2\",} 0.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/datalake/asset3\",} 2.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_dirs{path=\"/datalake/asset1\",} 0.0"));

        assertTrue(body.contains("fsimage_path_links{path=\"/datalake/asset2\",} 0.0"));
        assertTrue(body.contains("fsimage_path_links{path=\"/datalake/asset3\",} 0.0"));
        assertTrue(body.contains("fsimage_path_links{path=\"/user/mm\",} 0.0"));
        assertTrue(body.contains("fsimage_path_links{path=\"/datalake/asset1\",} 0.0"));

        // Path sets
        //  'userMmAndFooAndAsset1' : [
        //      '/datalake/asset3',
        //      '/user/mm',
        //      '/user/foo'
        //  ]
        assertTrue(body.contains("fsimage_path_set_dirs{path_set=\"userMmAndFooAndAsset1\",} 2.0"));
        assertTrue(body.contains("fsimage_path_set_blocks{path_set=\"userMmAndFooAndAsset1\",} 3.0"));
        assertTrue(body.contains("fsimage_path_set_links{path_set=\"userMmAndFooAndAsset1\",} 0.0"));
        assertTrue(body.contains("fsimage_path_set_fsize_count{path_set=\"userMmAndFooAndAsset1\",} 3.0"));
        assertTrue(body.contains("fsimage_path_set_fsize_sum{path_set=\"userMmAndFooAndAsset1\",} 6291456.0"));
        //  'datalakeAsset1and2' : [
        //      '/datalake/asset1',
        //      '/datalake/asset2'
        //  ]
        assertTrue(body.contains("fsimage_path_set_dirs{path_set=\"datalakeAsset1and2\",} 0.0"));
        assertTrue(body.contains("fsimage_path_set_blocks{path_set=\"datalakeAsset1and2\",} 2.0"));
        assertTrue(body.contains("fsimage_path_set_links{path_set=\"datalakeAsset1and2\",} 0.0"));
        assertTrue(body.contains("fsimage_path_set_fsize_count{path_set=\"datalakeAsset1and2\",} 2.0"));
        assertTrue(body.contains("fsimage_path_set_fsize_sum{path_set=\"datalakeAsset1and2\",} 2098176.0"));

        // Replication
        assertTrue(body.contains("fsimage_user_replication_count{user_name=\"root\",} 1.0"));
        assertTrue(body.contains("fsimage_user_replication_sum{user_name=\"root\",} 1.0"));
        assertTrue(body.contains("fsimage_user_replication_count{user_name=\"foo\",} 1.0"));
        assertTrue(body.contains("fsimage_user_replication_sum{user_name=\"foo\",} 1.0"));
        assertTrue(body.contains("fsimage_user_replication_count{user_name=\"mm\",} 14.0"));
        assertTrue(body.contains("fsimage_user_replication_sum{user_name=\"mm\",} 20.0"));


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
