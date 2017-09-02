package de.m3y.prometheus.exporter.fsimage;

import java.util.Set;

/**
 * Config options for collector.
 */
public class Config {
    /**
     * Path where HDFS NameNode stores fsimage file snapshots
     */
    private String fsImagePath;
    /**
     * Skip fsimage parsing if previously parsed.
     */
    private boolean skipPreviouslyParsed = true;
    /**
     * A list of paths to report statistics for.
     *
     * Paths can contain a regexp postfix, like "/users/ab.*", for matching direct child directories
     */
    private Set<String> paths;


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

    public Set<String> getPaths() {
        return paths;
    }

    public void setPaths(Set<String> paths) {
        this.paths = paths;
    }

    public boolean hasPaths() {
        return null != paths && ! paths.isEmpty();
    }
}
