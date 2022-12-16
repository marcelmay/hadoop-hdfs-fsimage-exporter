package de.m3y.prometheus.exporter.fsimage;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class ConfigTest {
    @Test
    public void testReadYml() throws IOException {
        Config config;
        try (Reader reader = new InputStreamReader(
                Thread.currentThread().getContextClassLoader().getResourceAsStream("config.yml"))) {
            config = new Yaml().loadAs(reader, Config.class);
        }

        assertThat(config.getFsImagePath()).isEqualTo("src/test/resources");

        assertThat(config.isSkipFileDistributionForGroupStats()).isFalse();
        assertThat(config.isSkipFileDistributionForUserStats()).isFalse();

        // Paths
        assertThat(config.hasPaths()).isTrue();
        assertThat(config.getPaths()).containsExactlyInAnyOrder("/tmp", "/datalake/a.*", "/user/m.*");

        assertThat(config.isSkipFileDistributionForPathStats()).isTrue();

        // PathSets
        assertThat(config.hasPathSets()).isTrue();
        final Map<String, List<String>> pathSets = config.getPathSets();
        assertThat(pathSets).hasSize(2);
        assertThat(pathSets.get("userMmAndFooAndAsset1")).
                containsExactlyInAnyOrder("/datalake/asset3", "/user/mm", "/user/foo");
        assertThat(pathSets.get("datalakeAsset1and2"))
                .containsExactlyInAnyOrder("/datalak.?/asset[2]", "/datalake/asset1");
        assertThat(config.isSkipFileDistributionForPathSetStats()).isTrue();

        // File size distribution
        final List<String> buckets = config.getFileSizeDistributionBuckets();
        assertThat(buckets).isEqualTo(Arrays.asList("0", "42", "1MiB", "32MiB", "64MiB", "128MiB", "1GiB", "12GiB"));
        assertThat(config.getFileSizeDistributionBucketsAsDoubles()).isEqualTo(new double[]{
                0, 42, 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024,
                1024L * 1024L * 1024L, 12L * 1024L * 1024L * 1024L
        });
    }


    @Test
    public void testConfigDefaults() {
        final Config config = new Config();

        assertThat(config.getFileSizeDistributionBucketsAsDoubles()).isEqualTo(new double[]{
                0, 1024 * 1024, 32 * 1024 * 1024, 64 * 1024 * 1024, 128 * 1024 * 1024,
                1024L * 1024L * 1024L, 10L * 1024L * 1024L * 1024L
        });
    }
}
