package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import de.m3y.hadoop.hdfs.hfsa.core.FsImageData;
import de.m3y.hadoop.hdfs.hfsa.core.FsImageLoader;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class FsImageReporterTest {

    @Test
    public void testExpandPaths() throws IOException {
        RandomAccessFile file = new RandomAccessFile("src/test/resources/fsimage_0001", "r");
        final FsImageData fsImageData = new FsImageLoader.Builder()
                .build()
                .load(file);
        final Set<String> paths = FsImageReporter.expandPaths(fsImageData,
                new HashSet<>(Arrays.asList("/tmp" /* Non existent */, "/user/m.*", "/datalake/a.*")));
        assertThat(paths).contains("/datalake/asset3", "/datalake/asset1", "/datalake/asset2", "/user/mm");
    }
}
