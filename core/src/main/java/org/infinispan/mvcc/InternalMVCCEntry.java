package org.infinispan.mvcc;

import org.infinispan.container.entries.InternalCacheEntry;

/**
 * @author pedro
 *         Date: 25-07-2011
 */
public class InternalMVCCEntry {
    private InternalCacheEntry value;
    private VersionVC visible;
    private boolean mostRecent;

    public InternalMVCCEntry(InternalCacheEntry value, VersionVC visible, boolean mostRecent) {
        this.value = value;
        this.visible = visible;
        this.mostRecent = mostRecent;
    }

    public InternalMVCCEntry(boolean mostRecent) {
        this(null, null, mostRecent);
    }

    public InternalCacheEntry getValue() {
        return value;
    }

    public VersionVC getVersion() {
        return visible;
    }

    public boolean isMostRecent() {
        return mostRecent;
    }

    @Override
    public String toString() {
        return new StringBuilder("{InternalMVCCEntry")
                .append(" value=").append(value)
                .append(",version=").append(visible)
                .append(",mostRecent?=").append(mostRecent)
                .append("}").toString();
    }
}
