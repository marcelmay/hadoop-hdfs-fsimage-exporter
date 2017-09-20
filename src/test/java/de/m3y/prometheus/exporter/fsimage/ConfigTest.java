package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.yaml.snakeyaml.Yaml;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.*;

/**
 *
 */
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
    }
}
