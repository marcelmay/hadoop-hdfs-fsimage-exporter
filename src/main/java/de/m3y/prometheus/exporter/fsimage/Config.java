package de.m3y.prometheus.exporter.fsimage;

/**
 * Config options for collector.
 */
public class Config {
    /** Path where HDFS NameNode stores fsimage file snapshots */
    private String fsImagePath;
    /** Skip fsimage parsing if previously parsed. */
    private boolean skipPreviouslyParsed = true;

    public String getFsImagePath() {
        return fsImagePath;
    }

    public void setFsImagePath(String fsImagePath) {
        this.fsImagePath = fsImagePath;
    }

    public boolean isSkipPreviouslyParsed() {
        return skipPreviouslyParsed;
    }

    public void setSkipPreviouslyParsed(boolean skipPreviouslyParsed) {
        this.skipPreviouslyParsed = skipPreviouslyParsed;
    }
}
