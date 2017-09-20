package de.m3y.prometheus.exporter.fsimage;

import java.util.List;
import java.util.Map;
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
     * <p>
     * Paths can contain a regexp postfix, like "/users/ab.*", for matching direct child directories
     */
    private Set<String> paths;
    private Map<String, List<String>> pathSets;

    /**
     * Skip file size distribution for group stats.
     */
    boolean skipFileDistributionForGroupStats = false;
    /**
     * Skip file size distribution for user stats.
     */
    boolean skipFileDistributionForUserStats = false;
    /**
     * Skip file size distribution for path based stats.
     */
    boolean skipFileDistributionForPathStats = false;
    /**
     * Skip file size distribution for path set based stats.
     */
    boolean skipFileDistributionForPathSetStats = false;

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
        return null != paths && !paths.isEmpty();
    }

    public boolean isSkipFileDistributionForPathStats() {
        return skipFileDistributionForPathStats;
    }

    public void setSkipFileDistributionForPathStats(boolean skipFileDistributionForPathStats) {
        this.skipFileDistributionForPathStats = skipFileDistributionForPathStats;
    }

    public boolean isSkipFileDistributionForGroupStats() {
        return skipFileDistributionForGroupStats;
    }

    public void setSkipFileDistributionForGroupStats(boolean skipFileDistributionForGroupStats) {
        this.skipFileDistributionForGroupStats = skipFileDistributionForGroupStats;
    }

    public boolean isSkipFileDistributionForUserStats() {
        return skipFileDistributionForUserStats;
    }

    public void setSkipFileDistributionForUserStats(boolean skipFileDistributionForUserStats) {
        this.skipFileDistributionForUserStats = skipFileDistributionForUserStats;
    }

    public Map<String, List<String>> getPathSets() {
        return pathSets;
    }

    public void setPathSets(Map<String, List<String>> pathSets) {
        this.pathSets = pathSets;
    }

    public boolean hasPathSets() {
        return pathSets != null && !pathSets.isEmpty();
    }

    public boolean isSkipFileDistributionForPathSetStats() {
        return skipFileDistributionForPathSetStats;
    }

    public void setSkipFileDistributionForPathSetStats(boolean skipFileDistributionForPathSetStats) {
        this.skipFileDistributionForPathSetStats = skipFileDistributionForPathSetStats;
    }
}
