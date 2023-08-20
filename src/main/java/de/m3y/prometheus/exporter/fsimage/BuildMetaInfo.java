package de.m3y.prometheus.exporter.fsimage;

/**
 * Holds build info such as version, build time etc.
 *
 * The actual values get injected during compile/build time via inject-maven-plugin.
 */
enum BuildMetaInfo {
    INSTANCE; // Enum Singleton

    public String getVersion() {
        return "<will be replaced>"; // NOSONAR
    }

    public String getBuildTimeStamp() {
        return "<will be replaced>"; // NOSONAR
    }

    public String getBuildScmVersion() {
        return "<will be replaced>"; // NOSONAR
    }

    public String getBuildScmBranch() {
        return "<will be replaced>"; // NOSONAR
    }
}

