package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

public class ConfigTest {
    @Test
    public void testReadYml() throws IOException {
        Config config;
        try (Reader reader = new InputStreamReader(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("config.yml"))) {
            config = new Yaml().loadAs(reader, Config.class);
        }

        assertEquals("src/test/resources", config.getFsImagePath());
        assertTrue(config.isSkipPreviouslyParsed());
        assertTrue(!config.isSkipFileDistributionForGroupStats());
        assertTrue(!config.isSkipFileDistributionForUserStats());

        // Paths
        assertTrue(config.hasPaths());
        assertThat(config.getPaths(),
                containsInAnyOrder("/tmp", "/datalake/a.*", "/user/m.*"));
        assertTrue(config.isSkipFileDistributionForPathStats());

        // PathSets
        assertTrue(config.hasPathSets());
        final Map<String, List<String>> pathSets = config.getPathSets();
        assertEquals(2, pathSets.size());
        assertThat(pathSets.get("userMmAndFooAndAsset1"),
                containsInAnyOrder("/datalake/asset3", "/user/mm", "/user/foo"));
        assertThat(pathSets.get("datalakeAsset1and2"),
                containsInAnyOrder("/datalake/asset2", "/datalake/asset1"));
        assertTrue(config.isSkipFileDistributionForPathSetStats());

        // File size distribution
        final List<String> buckets = config.getFileSizeDistributionBuckets();
        assertEquals(buckets, Arrays.asList("0", "42", "1MiB", "32MiB", "64MiB", "128MiB", "1GiB", "12GiB"));
        assertArrayEquals(config.getFileSizeDistributionBucketsAsDoubles(), new double[]{
                0, 42, 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024,
                1024L * 1024L * 1024L, 12L * 1024L * 1024L * 1024L
        }, 1e-2);
    }


    @Test
    public void testConfigDefaults() {
        final Config config = new Config();

        assertArrayEquals(config.getFileSizeDistributionBucketsAsDoubles(), new double[]{
                0, 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024,
                1024L * 1024L * 1024L, 10L * 1024L * 1024L * 1024L
        }, 1e-2);

    }
}
