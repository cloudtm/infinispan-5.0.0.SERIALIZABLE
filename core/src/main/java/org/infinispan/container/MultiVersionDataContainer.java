package org.infinispan.container;

import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalEntryFactory;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.mvcc.CommitLog;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.util.Immutables;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author pedro
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 */
public class MultiVersionDataContainer implements DataContainer {
    /*
     * This class is not finished yet! it has some bugs and I don't know how to expire the key
     * Beside that, a garbage collector is needed to remove old versions
     * ... and some other things that I don't remember
     *
     * note: this class has big performance issue. it is recomended to redo it in a better way
     */

    private static final Log log = LogFactory.getLog(MultiVersionDataContainer.class);

    private CommitLog commitLog;
    private final InternalEntryFactory entryFactory;
    private final ConcurrentMap<Object, VBox> entries;
    
    
    private VersionVCFactory vf;

    private boolean trace, debug;

    public MultiVersionDataContainer(int concurrencyLevel) {

        log.info("GMU algorithm");

        entryFactory = new InternalEntryFactory();
        entries = new ConcurrentHashMap<Object, VBox>(128, 0.75f, concurrencyLevel);
    }
    
    @Inject
    public void inject(VersionVCFactory vf){
    	
    	this.vf=vf;
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

    private VBox getFromMap(Object k, long max) {
        VBox vbox = entries.get(k);
        
        if(max < 0){
        	return vbox;
        }
        
        while(vbox != null) {
            if(vbox.version <= max) {
                break;
            } else {
                vbox = vbox.previous;
            }
        }
        return vbox;
    }

    @Inject
    public void inject(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    @Override
    public InternalCacheEntry get(Object k) {
        return get(k, null, true).getValue();
    }

    @Override
    public InternalCacheEntry peek(Object k) {
        return peek(k, null, true).getValue();
    }

    @Override
    public void put(Object k, Object v, long lifespan, long maxIdle) {
        put(k, v, lifespan, maxIdle, commitLog.getActualVersion());
    }

    @Override
    public boolean containsKey(Object k) {
        InternalCacheEntry ice = peek(k, null, true).getValue();
        return ice != null;
    }

    @Override
    public InternalCacheEntry remove(Object k) {
        return remove(k, commitLog.getActualVersion());
    }

    @Override
    public int size() {
        return size(null, true);
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
        return values(null, true);
    }

    @Override
    public Set<InternalCacheEntry> entrySet() {
        return new EntrySet(null);
    }

    @Override
    public void purgeExpired() {
        purgeExpired(null, true);
    }

    @Override
    public InternalMVCCEntry get(Object k, VersionVC max, boolean firstTimeOnNode) {
    	VersionVC visible;
    	if(firstTimeOnNode){
    		visible = commitLog.getMostRecentLessOrEqualThan(max);
    	}
    	else{
    		
			visible = max;
			
    	}
    	
    	
    	
     //   synchronized (entries) {
            VBox vbox = getFromMap(k,visible.get(this.vf.getMyIndex()));
            InternalMVCCEntry ime = wrap(vbox, visible, vbox == entries.get(k), true, false);
            /*
            if(ime.getValue() == null) {
                entries.remove(k);
            }
            */
            if(debug) {
                log.debugf("get key [%s] with max vector clock of %s. returned value is %s",
                        k, max, (ime.getValue() !=  null ? ime.getValue().getValue() : "null"));
            }
            return ime;
       // }
    }

    @Override
    public InternalMVCCEntry peek(Object k, VersionVC max, boolean firstTimeOnNode) {
    	
    	VersionVC visible;
    	if(firstTimeOnNode){
    		visible = commitLog.getMostRecentLessOrEqualThan(max);
    	}
    	else{
    		
			visible = max;
			
    	}
        
    	VBox vbox = getFromMap(k,visible.get(this.vf.getMyIndex()));
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
        VBox newVbox = new VBox(version.get(vf.getMyIndex()), e, prev);

      //  synchronized (entries) {
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
      //  }
    }

    @Override
    public boolean containsKey(Object k, VersionVC max, boolean firstTimeOnNode) {
    	
    	VersionVC visible;
    	if(firstTimeOnNode){
    		visible = commitLog.getMostRecentLessOrEqualThan(max);
    	}
    	else{
    		
			visible = max;
			
    	}
    	VBox vbox = getFromMap(k,visible.get(this.vf.getMyIndex()));
        InternalMVCCEntry ime = wrap(vbox, visible, vbox == entries.get(k), false, false);
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
        VBox newVbox = new VBox(version.get(vf.getMyIndex()), null, prev);

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
    public int size(VersionVC max, boolean firstTimeOnNode) {
        Set<Object> keys = entries.keySet();
        int size = 0;
        for(Object k : keys) {
            InternalMVCCEntry ime = peek(k, max, firstTimeOnNode);
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
    public Set<Object> keySet(VersionVC max, boolean firstTimeOnNode) {
        Set<Object> result = new HashSet<Object>();
        VersionVC visible;
    	if(firstTimeOnNode){
    		visible = commitLog.getMostRecentLessOrEqualThan(max);
    	}
    	else{
    		
			visible = max;
			
    	}
    	
    	long visibleValue = visible.get(this.vf.getMyIndex());

        for(Map.Entry<Object, VBox> entry : entries.entrySet()) {
            Object key = entry.getKey();
            VBox value = entry.getValue();
            
            
            if(visibleValue < 0){
            	result.add(key);
            	continue;
            }
            
            while(value != null) {
                if(value.getVersion() <= visibleValue) {
                	result.add(key);
                    break;
                } else {
                    value = value.getPrevious();
                }
            }
            
        }

        if(debug) {
            log.debugf("Key Set with max version %s is %s", max, result);
        }

        return Collections.unmodifiableSet(result);
    }

    @Override
    public Collection<Object> values(VersionVC max, boolean firstTimeOnNode) {
        if(trace) {
            log.tracef("Values with max version %s", max);
        }
        return new Values(max, size(max, firstTimeOnNode));
    }

    @Override
    public Set<InternalCacheEntry> entrySet(VersionVC max) {
        if(trace) {
            log.tracef("Entry Set with max version %s", max);
        }
        return new EntrySet(max);
    }

    @Override
    public void purgeExpired(VersionVC version, boolean firstTimeOnNode) {
        if(trace) {
            log.tracef("Purge Expired keys (remove with version %s)", version);
        }
        VersionVC visible;
    	if(firstTimeOnNode){
    		visible = commitLog.getMostRecentLessOrEqualThan(version);
    	}
    	else{
    		
			visible = version;
			
    	}
    	
    	long visibleValue = visible.get(this.vf.getMyIndex());
        
        for (Iterator<VBox> purgeCandidates = entries.values().iterator(); purgeCandidates.hasNext();) {
            VBox vbox = purgeCandidates.next();
            if (vbox.getVersion()<=visibleValue && vbox.isExpired()) {
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
        return new EntryIterator(new VBoxIterator(entries.values().iterator(), commitLog.getActualVersion(), vf.getMyIndex()));
    }

    public void addNewCommittedTransaction(VersionVC newVersion) {
        commitLog.addNewVersion(newVersion);
    }
/*
    public void addNewCommittedTransaction(List<VersionVC> newsVersions) {
        commitLog.addNewVersion(newsVersions);
    }
    
  */

    @Override
    public boolean validateKey(Object key, VersionVC version) {
        VBox actual = entries.get(key);
        if(actual == null) {
            if(debug) {
                log.debugf("Validate key [%s], but it is null in data container. return true", key);
            }
            return true;
        }
        long actualValue = actual.getVersion();
        
        long referenceValue = version.get(vf.getMyIndex());
        if(debug) {
            log.debugf("validate key [%s]. most recent version is %s. compare with value %s",
                    key, actual.getVersion(), referenceValue);
        }

        return actualValue <= referenceValue;
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
            return new ValueIterator(new VBoxIterator(entries.values().iterator(), version, vf.getMyIndex()));
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
                ice = get(e.getKey(), max, true).getValue();
            }

            return ice != null && ice.getValue().equals(e.getValue());
        }

        @Override
        public Iterator<InternalCacheEntry> iterator() {
            if(max != null) {
                return new ImmutableEntryIterator(new VBoxIterator(entries.values().iterator(), max, vf.getMyIndex()));
            } else {
                return new ImmutableEntryIterator(new VBoxIterator(entries.values().iterator(), commitLog.getActualVersion(), vf.getMyIndex()));
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
        long maxValue;

        private VBoxIterator(Iterator<VBox> it, VersionVC max, int referenceIndex) {
            currentIterator = it;
            next = null;
            this.max = max;
            this.maxValue = max.get(referenceIndex);
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
                    if(vbox.getVersion() <= maxValue) {
                        next = vbox;
                        return;
                    } else {
                        vbox = vbox.getPrevious();
                    }
                }
            }
        }
    }
    
    
    /**
     * @author pedro
     * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
     * @since 5.0        
     */
     class VBox {
    	

        long version;
        InternalCacheEntry value;
        VBox previous;

        public VBox(long version, InternalCacheEntry value, VBox previous) {
            this.version = version;
            this.value = value;
            this.previous = previous;
            updatedVersion();
        }

        public long getVersion() {
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
        		
        	//We need this line for non-transactional writes
        	if(previous.version > version){
        		version = previous.version;
        		
        	}
        	
        		
        	
        }
   

        public String getVBoxChain() {
            return new StringBuilder("VBox{")
                    .append("version=").append(version)
                    .append(",value=").append(value).append("};")
                    .append(previous != null ? previous.getVBoxChain() : "null")
                    .toString();
        }

        @Override
        public String toString() {
            return new StringBuilder("VBox{")
                    .append("version=").append(version)
                    .append(",value=").append(value)
                    .append('}').toString();
        }
    }
}
