package de.m3y.prometheus.exporter.fsimage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import de.m3y.hadoop.hdfs.hfsa.core.FSImageLoader;
import org.junit.Test;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

public class FsImageReporterTest {

    @Test
    public void testExpandPaths() throws IOException {
        RandomAccessFile file = new RandomAccessFile("src/test/resources/fsimage_0001", "r");
        final FSImageLoader loader = FSImageLoader.load(file);
        final Set<String> paths = FsImageReporter.expandPaths(loader,
                new HashSet<>(Arrays.asList("/tmp" /* Non existent */, "/user/m.*", "/datalake/a.*")));
        assertThat(paths, containsInAnyOrder("/datalake/asset3", "/datalake/asset1", "/datalake/asset2", "/user/mm"));
    }
}
