package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.mvcc.CommitLog;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VBox;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.Immutables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author pedro
 *         Date: 01-08-2011
 */
public class MultiVersionDataContainer implements DataContainer {
    /*
     * This class is not finished yet! it has some bugs and I don't know how to expire the key
     * Beside that, a garbage collector is needed to remove old versions
     * ... and some other things that I don't remember
     */

    private static final Log log = LogFactory.getLog(MultiVersionDataContainer.class);

    private CommitLog commitLog;
    private final InternalEntryFactory entryFactory;
    private final ConcurrentMap<Object, VBox> entries;

    private boolean trace, debug;

    public MultiVersionDataContainer(int concurrencyLevel) {
        entryFactory = new InternalEntryFactory();
        entries = new ConcurrentHashMap<Object, VBox>(128, 0.75f, concurrencyLevel);
    }

    @Start
    public void setLogBoolean() {
        trace = log.isTraceEnabled();
        debug = log.isDebugEnabled();
    }

    @Stop
    public void stop() {
        entries.clear();
    }

    private InternalMVCCEntry wrap(VBox vbox, VersionVC visible,  boolean mostRecent, boolean touch, boolean ignoreExpire) {
        if(vbox == null) {
            return new InternalMVCCEntry(visible, mostRecent);
        } else if(mostRecent && vbox.isExpired() && !ignoreExpire) {
            return new InternalMVCCEntry(visible, mostRecent);
        }
        return new InternalMVCCEntry(vbox.getValue(touch), visible, mostRecent);
    }

    private VBox getFromMap(Object k, VersionVC max) {
        VBox vbox = entries.get(k);
        while(vbox != null) {
            if(vbox.getVersion().isBefore(max)) {
                break;
            } else {
                vbox = vbox.getPrevious();
            }
        }
        return vbox;
    }

    //debug
    /*private static void printVBox(VBox vbox) {
        log.debugf("printing vbox chain: %s", vbox != null ? vbox.getVBoxChain() : "null");
    }*/

    @Inject
    public void inject(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    @Override
    public InternalCacheEntry get(Object k) {
        return get(k, null).getValue();
    }

    @Override
    public InternalCacheEntry peek(Object k) {
        return peek(k, null).getValue();
    }

    @Override
    public void put(Object k, Object v, long lifespan, long maxIdle) {
        put(k, v, lifespan, maxIdle, commitLog.getActualVersion());
    }

    @Override
    public boolean containsKey(Object k) {
        InternalCacheEntry ice = peek(k, null).getValue();
        return ice != null;
    }

    @Override
    public InternalCacheEntry remove(Object k) {
        return remove(k, commitLog.getActualVersion());
    }

    @Override
    public int size() {
        return size(null);
    }

    @Override
    public void clear() {
        entries.clear();
    }

    @Override
    public Set<Object> keySet() {
        return Collections.unmodifiableSet(entries.keySet());
    }

    @Override
    public Collection<Object> values() {
        return values(commitLog.getActualVersion());
    }

    @Override
    public Set<InternalCacheEntry> entrySet() {
        return new EntrySet(null);
    }

    @Override
    public void purgeExpired() {
        purgeExpired(commitLog.getActualVersion());
    }

    @Override
    public InternalMVCCEntry get(Object k, VersionVC max) {
        VersionVC visible = commitLog.getMostRecentLessOrEqualThan(max);
        VBox vbox = getFromMap(k,visible);
        InternalMVCCEntry ime = wrap(vbox, visible, vbox == entries.get(k), true, false);
        if(ime.getValue() == null) {
            entries.remove(k);
        }
        if(debug) {
            log.debugf("get key [%s] with max vector clock of %s. returned value is %s",
                    k, max, (ime.getValue() !=  null ? ime.getValue().getValue() : "null"));
        }
        return ime;
    }

    @Override
    public InternalMVCCEntry peek(Object k, VersionVC max) {
        VersionVC visible = commitLog.getMostRecentLessOrEqualThan(max);
        VBox vbox = getFromMap(k,visible);
        InternalMVCCEntry ime = wrap(vbox, visible, vbox == entries.get(k), false, true);
        if(debug) {
            log.debugf("peek key [%s] with max vector clock of %s. returned value is %s",
                    k, max, (ime.getValue() !=  null ? ime.getValue().getValue() : "null"));
        }
        return ime;
    }

    @Override
    public void put(Object k, Object v, long lifespan, long maxIdle, VersionVC version) {
        VBox prev = entries.get(k);
        InternalCacheEntry e = entryFactory.createNewEntry(k, v, lifespan, maxIdle);
        VBox newVbox = new VBox(version, e, prev);

        //if the entry does not exist
        if(prev == null) {
            prev = entries.putIfAbsent(k, newVbox);
            if(prev == null) {
                if(debug) {
                    log.debugf("added new value to key [%s] with version %s and value %s", k, newVbox.getVersion(), v);
                }
                return ;
            }
            //ops... maybe it exists now... lets replace it
            newVbox.setPrevious(prev);
            newVbox.updatedVersion();
        }

        while(!entries.replace(k, prev, newVbox)) {
            prev = entries.get(k);
            newVbox.setPrevious(prev);
            newVbox.updatedVersion();
        }
        if(debug) {
            log.debugf("added new value to key [%s] with version %s and value %s", k, newVbox.getVersion(), v);
        }
    }

    @Override
    public boolean containsKey(Object k, VersionVC max) {
        VBox vbox = getFromMap(k,max);
        InternalMVCCEntry ime = wrap(vbox, commitLog.getMostRecentLessOrEqualThan(max), vbox == entries.get(k), false, false);
        if(ime.getValue() == null) {
            entries.remove(k);
            if(debug) {
                log.debugf("Contains key [%s] with max version %s. results is false",
                        k, max);
            }
            return false;
        }
        if(debug) {
            log.debugf("Contains key [%s] with max version %s. results is true",
                    k, max);
        }
        return true;
    }

    @Override
    public InternalCacheEntry remove(Object k, VersionVC version) {
        VBox prev = entries.get(k);
        VBox newVbox = new VBox(version, null, prev);

        //if the entry does not exist
        if(prev == null) {
            prev = entries.putIfAbsent(k, newVbox);
            if(prev == null) {
                if(debug) {
                    log.debugf("Remove key [%s]. Create empty value with version %s",
                            k, version);
                }
                return null;
            }
            //ops... maybe it exists now... lets replace it
            newVbox.setPrevious(prev);
            newVbox.updatedVersion();
        }

        while(!entries.replace(k, prev, newVbox)) {
            prev = entries.get(k);
            newVbox.setPrevious(prev);
            newVbox.updatedVersion();
        }

        if(debug) {
            log.debugf("Remove key [%s]. Create empty value with version %s",
                    k, newVbox.getVersion());
        }
        return prev == null || prev.getValue(false) == null || prev.getValue(false).isExpired() ? null : prev.getValue(false);
    }

    @Override
    public int size(VersionVC max) {
        Set<Object> keys = entries.keySet();
        int size = 0;
        for(Object k : keys) {
            InternalMVCCEntry ime = peek(k, max);
            if(ime.getValue() != null) {
                size++;
            }
        }
        if(debug) {
            log.debugf("Size of map with max version of %s is %s", max, size);
        }
        return size;
    }

    @Override
    public void clear(VersionVC version) {
        if(trace) {
            log.tracef("Clear the map (i.e. remove all key by putting a new empty value version");
        }
        Set<Object> keys = entries.keySet();
        for(Object k : keys) {
            remove(k,version);
        }
    }

    @Override
    public Set<Object> keySet(VersionVC max) {
        Set<Object> result = new HashSet<Object>();

        for(Map.Entry<Object, VBox> entry : entries.entrySet()) {
            Object key = entry.getKey();
            VBox value = entry.getValue();
            while(value != null) {
                if(value.getVersion().isBefore(max)) {
                    result.add(key);
                    break;
                }
            }
        }

        if(debug) {
            log.debugf("Key Set with max version %s is %s", max, result);
        }

        return Collections.unmodifiableSet(result);
    }

    @Override
    public Collection<Object> values(VersionVC max) {
        if(trace) {
            log.tracef("Values with max version %s", max);
        }
        return new Values(max, size(max));
    }

    @Override
    public Set<InternalCacheEntry> entrySet(VersionVC max) {
        if(trace) {
            log.tracef("Entry Set with max version %s", max);
        }
        return new EntrySet(max);
    }

    @Override
    public void purgeExpired(VersionVC version) {
        if(trace) {
            log.tracef("Purge Expired keys (remove with version %s)", version);
        }
        for (Iterator<VBox> purgeCandidates = entries.values().iterator(); purgeCandidates.hasNext();) {
            VBox vbox = purgeCandidates.next();
            if (vbox.getVersion().isBefore(version) && vbox.isExpired()) {
                purgeCandidates.remove();
            }
        }
    }

    @Override
    public Iterator<InternalCacheEntry> iterator() {
        //this can be a problem...
        if(trace) {
            log.tracef("Iterator with actual version");
        }
        return new EntryIterator(new VBoxIterator(entries.values().iterator(), commitLog.getActualVersion()));
    }

    public void addNewCommittedTransaction(VersionVC newVersion) {
        commitLog.addNewVersion(newVersion);
    }

    public void addNewCommittedTransaction(List<VersionVC> newsVersions) {
        commitLog.addNewVersion(newsVersions);
    }

    @Override
    public boolean validateKey(Object key, int idx, long value) {
        VBox actual = entries.get(key);
        if(actual == null) {
            if(debug) {
                log.debugf("Validate key [%s], but it is null in data container. return true", key);
            }
            return true;
        }
        long actualValue = actual.getVersion().get(idx);

        if(debug) {
            log.debugf("validate key [%s]. most recent version is %s. compare with value %s in position %s",
                    key, actual.getVersion(), value, idx);
        }

        return actualValue <= value;
    }

    /**
     * Minimal implementation needed for unmodifiable Collection
     *
     */
    private class Values extends AbstractCollection<Object> {
        private VersionVC version;
        private int size;

        private Values(VersionVC version, int size) {
            this.version = version;
            this.size = size;
        }

        @Override
        public Iterator<Object> iterator() {
            return new ValueIterator(new VBoxIterator(entries.values().iterator(), version));
        }

        @Override
        public int size() {
            return size;
        }
    }

    private static class ValueIterator implements Iterator<Object> {
        Iterator<VBox> currentIterator;

        private ValueIterator(Iterator<VBox> it) {
            currentIterator = it;
        }

        public boolean hasNext() {
            return currentIterator.hasNext();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public Object next() {
            return currentIterator.next().getValue(false).getValue();
        }
    }

    private static class EntryIterator implements Iterator<InternalCacheEntry> {
        Iterator<VBox> currentIterator;

        private EntryIterator(Iterator<VBox> it) {
            currentIterator = it;
        }

        public boolean hasNext() {
            return currentIterator.hasNext();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public InternalCacheEntry next() {
            return currentIterator.next().getValue(false);
        }
    }

    private static class ImmutableEntryIterator extends EntryIterator {
        ImmutableEntryIterator(Iterator<VBox> it){
            super(it);
        }

        @Override
        public InternalCacheEntry next() {
            return Immutables.immutableInternalCacheEntry(super.next());
        }
    }

    private class EntrySet extends AbstractSet<InternalCacheEntry> {

        private VersionVC max;

        public EntrySet(VersionVC max) {
            this.max = max;
        }

        @Override
        public boolean contains(Object o) {
            if (!(o instanceof Map.Entry)) {
                return false;
            }

            @SuppressWarnings("rawtypes")
            Map.Entry e = (Map.Entry) o;
            InternalCacheEntry ice;
            if(max == null) {
                ice = get(e.getKey());
            } else {
                ice = get(e.getKey(), max).getValue();
            }

            return ice != null && ice.getValue().equals(e.getValue());
        }

        @Override
        public Iterator<InternalCacheEntry> iterator() {
            if(max != null) {
                return new ImmutableEntryIterator(new VBoxIterator(entries.values().iterator(), max));
            } else {
                return new ImmutableEntryIterator(new VBoxIterator(entries.values().iterator(), commitLog.getActualVersion()));
            }
        }

        @Override
        public int size() {
            return entries.size();
        }
    }

    private static class VBoxIterator implements Iterator<VBox> {
        Iterator<VBox> currentIterator;
        VBox next;
        VersionVC max;

        private VBoxIterator(Iterator<VBox> it, VersionVC max) {
            currentIterator = it;
            next = null;
            this.max = max;
            findNext();
        }

        public boolean hasNext() {
            return next != null;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public VBox next() {
            if(next == null) {
                throw new NoSuchElementException();
            }
            VBox vbox = next;
            findNext();
            return vbox;
        }

        private void findNext() {
            next = null;
            while(currentIterator.hasNext()) {
                VBox vbox = currentIterator.next();
                while(vbox != null) {
                    if(vbox.getVersion().isBefore(max)) {
                        next = vbox;
                        return;
                    } else {
                        vbox = vbox.getPrevious();
                    }
                }
            }
        }
    }
}
