package de.m3y.prometheus.exporter.fsimage;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageLoader;
import org.junit.Test;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class FsImageReporterTest {

    @Test
    public void testExpandPaths() throws IOException {
        RandomAccessFile file = new RandomAccessFile("src/test/resources/fsimage_0001", "r");
        final FsImageData fsImageData = new FsImageLoader.Builder()
                .build()
                .load(file);
        Set<String> paths = FsImageReporter.expandPaths(fsImageData,
                new HashSet<>(Arrays.asList("/tmp" /* Non existent */, "/user/m.*", "/datalake/a.*")));
        assertThat(paths).contains("/datalake/asset3", "/datalake/asset1", "/datalake/asset2", "/user/mm");

        paths = FsImageReporter.expandPaths(fsImageData,
                new HashSet<>(Collections.singletonList("/datalake/.*")));
        assertThat(paths)
                .hasSize(3)
                .contains("/datalake/asset3", "/datalake/asset1", "/datalake/asset2");

        paths = FsImageReporter.expandPaths(fsImageData,
                new HashSet<>(Collections.singletonList("/datalake/.*[2,3]")));
        assertThat(paths)
                .hasSize(2)
                .contains("/datalake/asset3",  "/datalake/asset2");

        paths = FsImageReporter.expandPaths(fsImageData,
                new HashSet<>(Collections.singletonList("/datal.*e/.*")));
        assertThat(paths)
                .hasSize(3)
                .contains( "/datalake/asset1", "/datalake/asset2", "/datalake/asset3");

        paths = FsImageReporter.expandPaths(fsImageData,
                new HashSet<>(Collections.singletonList("/test.*/foo")));
        assertThat(paths)
                .hasSize(1)
                .contains("/test3/foo");

        paths = FsImageReporter.expandPaths(fsImageData,
                new HashSet<>(Collections.singletonList("/test3/.*/.*")));
        assertThat(paths)
                .hasSize(1)
                .contains("/test3/foo/bar");
    }
}
