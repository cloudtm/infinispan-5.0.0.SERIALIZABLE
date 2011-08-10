package org.infinispan.container.entries;

import org.infinispan.atomic.AtomicHashMap;
import org.infinispan.container.DataContainer;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author pruivo
 *         Date: 10/08/11
 */
public class SerializableEntry extends ReadCommittedEntry {
    private static final Log log = LogFactory.getLog(ReadCommittedEntry.class);
    private static final boolean trace = log.isTraceEnabled();

    public SerializableEntry(Object key, Object value, long lifespan) {
        super(key,value,lifespan);
    }

    public final void commit(DataContainer container, VersionVC newVersion) {
        // only do stuff if there are changes.
        if (isChanged()) {
            if (trace){
                log.tracef("Updating entry (key=%s removed=%s valid=%s changed=%s created=%s value=%s]", getKey(),
                        isRemoved(), isValid(), isChanged(), isCreated(), value);
            }

            // Ugh!
            if (value instanceof AtomicHashMap) {
                AtomicHashMap ahm = (AtomicHashMap) value;
                ahm.commit();
                if (isRemoved() && !isEvicted()) {
                    ahm.markRemoved(true);
                }
            }

            if (isRemoved()) {
                container.remove(key, newVersion);
            } else if (value != null) {
                container.put(key, value, getLifespan(), getMaxIdle(), newVersion);
            }
            reset();
        }
    }

    private void reset() {
        oldValue = null;
        flags = 0;
        setValid(true);
    }
}
