package de.m3y.prometheus.exporter.fsimage;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class FsImageWatcherTest {

    @Test
    public void testFindLatestFSImageFile() throws IOException {
        File tempDirectory = Files.createTempDirectory("findLatestFSImageFile").toFile();
        tempDirectory.deleteOnExit();

        // Non-existent dir
        try {
            FsImageWatcher.findLatestFSImageFile(new File(tempDirectory, "non-existent"));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException ex) {
            // expected
        }

        // Empty dir
        try {
            FsImageWatcher.findLatestFSImageFile(tempDirectory);
            fail("Expected IllegalStateException");
        } catch (IllegalStateException ex) {
            // expected
        }

        final File file_1 = createTmpFile(tempDirectory, "fsimage_0000000001650677390");

        assertEquals(file_1, FsImageWatcher.findLatestFSImageFile(tempDirectory));

        final File file_2 = createTmpFile(tempDirectory, "fsimage_0000000001650677391");
        assertEquals(file_2, FsImageWatcher.findLatestFSImageFile(tempDirectory));

        final File file_3 = createTmpFile(tempDirectory, "fsimage_1000000001650677391");
        assertEquals(file_3, FsImageWatcher.findLatestFSImageFile(tempDirectory));
    }

    private File createTmpFile(File tempDirectory, String fileName) throws IOException {
        final File newFile = new File(tempDirectory, fileName);
        newFile.createNewFile();
        newFile.deleteOnExit();
        return newFile;
    }
}
