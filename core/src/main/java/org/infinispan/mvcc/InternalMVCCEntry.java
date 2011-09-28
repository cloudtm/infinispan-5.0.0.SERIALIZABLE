package org.infinispan.mvcc;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

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

    public InternalMVCCEntry(VersionVC visible, boolean mostRecent) {
        this(null, visible, mostRecent);
    }

    public InternalCacheEntry getValue() {
        return (value != null ? value.clone() : null);
    }

    public VersionVC getVersion() {
        return visible;
    }

    public boolean isMostRecent() {
        return mostRecent;
    }

    @Override
    public String toString() {
        return new StringBuilder("InternalMVCCEntry{")
                .append(" value=").append(value)
                .append(",version=").append(visible)
                .append(",mostRecent?=").append(mostRecent)
                .append("}").toString();
    }

    public static class Externalizer extends AbstractExternalizer<InternalMVCCEntry> {

        @Override
        public Set<Class<? extends InternalMVCCEntry>> getTypeClasses() {
            return Util.<Class<? extends InternalMVCCEntry>>asSet(InternalMVCCEntry.class);
        }

        @Override
        public void writeObject(ObjectOutput output, InternalMVCCEntry object) throws IOException {
            output.writeObject(object.value);
            output.writeObject(object.visible);
            output.writeBoolean(object.mostRecent);
        }

        @Override
        public InternalMVCCEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
            InternalCacheEntry ice = (InternalCacheEntry) input.readObject();
            VersionVC visible = (VersionVC) input.readObject();
            boolean mostRecent = input.readBoolean();

            return new InternalMVCCEntry(ice, visible, mostRecent);
        }

        @Override
        public Integer getId() {
            return Ids.INTERNAL_MVCC_ENTRY;
        }
    }
}
