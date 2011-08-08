package org.infinispan.mvcc;

import org.infinispan.container.entries.InternalCacheEntry;

/**
 * @author pedro
 *         Date: 25-07-2011
 */
public class VBox {

    private VersionVC version;
    private InternalCacheEntry value;
    private VBox previous;

    public VBox(VersionVC version, InternalCacheEntry value, VBox previous) {
        this.version = version;
        this.value = value;
        this.previous = previous;
        updatedVersion();
    }

    public VersionVC getVersion() {
        return version;
    }

    public InternalCacheEntry getValue(boolean touch) {
        if(touch && value != null) {
            value.touch();
        }
        return value;
    }

    public VBox getPrevious() {
        return previous;
    }

    public void setPrevious(VBox previous) {
        this.previous = previous;
    }

    public boolean isExpired() {
        return value != null && value.isExpired();
    }

    public void updatedVersion() {
        if(previous == null) {
            return ;
        }
        VersionVC cp = version.copy();
        cp.setToMaximum(previous.version);
        version = cp;
    }
}
