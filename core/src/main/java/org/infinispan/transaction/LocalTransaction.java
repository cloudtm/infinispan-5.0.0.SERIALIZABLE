/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.infinispan.transaction;

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.ReadSetEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.remoting.transport.Address;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.InfinispanCollections;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import java.util.*;

/**
 * Object that holds transaction's state on the node where it originated; as opposed to {@link RemoteTransaction}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public abstract class LocalTransaction extends AbstractCacheTransaction {

    private static final Log log = LogFactory.getLog(LocalTransaction.class);
    private static final boolean trace = log.isTraceEnabled();

    private Set<Address> remoteLockedNodes;

    /** mark as volatile as this might be set from the tx thread code on view change*/
    private volatile boolean isMarkedForRollback;

    private final Transaction transaction;

    private VersionVC commitVersion;

    private boolean locallyValidated = false;
    
    

    public LocalTransaction(Transaction transaction, GlobalTransaction tx) {
        super.tx = tx;
        this.transaction = transaction;
    }

    public void addModification(WriteCommand mod) {
        if (trace) log.tracef("Adding modification %s. Mod list is %s", mod, modifications);
        if (modifications == null) {
            modifications = new LinkedList<WriteCommand>();
        }
        modifications.add(mod);
    }

    public boolean hasRemoteLocksAcquired(Collection<Address> leavers) {
        if (trace) {
            log.tracef("My remote locks: %s, leavers are: %s", remoteLockedNodes, leavers);
        }
        return (remoteLockedNodes != null) && !Collections.disjoint(remoteLockedNodes, leavers);
    }

    public void locksAcquired(Collection<Address> nodes) {
        if (remoteLockedNodes == null) remoteLockedNodes = new HashSet<Address>();
        remoteLockedNodes.addAll(nodes);
    }

    public Collection<Address> getRemoteLocksAcquired(){
        if (remoteLockedNodes == null) return Collections.emptySet();
        return remoteLockedNodes;
    }

    public void filterRemoteLocksAcquire(Collection<Address> existingMembers) {
        Iterator<Address> it = getRemoteLocksAcquired().iterator();
        while (it.hasNext()) {
            Address next = it.next();
            if (!existingMembers.contains(next))
                it.remove();
        }
    }

    public void clearRemoteLocksAcquired() {
        if (remoteLockedNodes != null) remoteLockedNodes.clear();
    }

    public void markForRollback(boolean markForRollback) {
        isMarkedForRollback = markForRollback;
    }

    public boolean isMarkedForRollback() {
        return isMarkedForRollback;
    }

    public Transaction getTransaction() {
        return transaction;
    }

    public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
        return (BidirectionalMap<Object, CacheEntry>)
                (lookedUpEntries == null ? InfinispanCollections.emptyBidirectionalMap() : lookedUpEntries);
    }

    public void putLookedUpEntry(Object key, CacheEntry e) {
        if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
        lookedUpEntries.put(key, e);
    }

    public boolean isReadOnly() {
        return (modifications == null || modifications.isEmpty()) && (lookedUpEntries == null || lookedUpEntries.isEmpty());
    }
    
    

    public abstract boolean isEnlisted();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocalTransaction that = (LocalTransaction) o;

        if (isMarkedForRollback != that.isMarkedForRollback) return false;
        if (remoteLockedNodes != null ? !remoteLockedNodes.equals(that.remoteLockedNodes) : that.remoteLockedNodes != null)
            return false;
        if (transaction != null ? !transaction.equals(that.transaction) : that.transaction != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = remoteLockedNodes != null ? remoteLockedNodes.hashCode() : 0;
        result = 31 * result + (isMarkedForRollback ? 1 : 0);
        result = 31 * result + (transaction != null ? transaction.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "LocalTransaction{" +
                "remoteLockedNodes=" + remoteLockedNodes +
                ", isMarkedForRollback=" + isMarkedForRollback +
                ", transaction=" + transaction +
                "} " + super.toString();
    }

    public void setModifications(List<WriteCommand> modifications) {
        this.modifications = modifications;
    }

    @Override
    public void addLocalReadKey(Object key, InternalMVCCEntry ime) {
        if(localReadSet == null) {
            localReadSet = new LinkedList<ReadSetEntry>();
        }
        localReadSet.addLast(new ReadSetEntry(key, ime));
    }
    
    @Override
    public void removeLocalReadKey(Object key) {
        if(localReadSet != null){
        	Iterator<ReadSetEntry> itr = localReadSet.descendingIterator();
        	while(itr.hasNext()){
        		ReadSetEntry entry = itr.next();
        		
        		if(entry.getKey() != null && entry.getKey().equals(key)){
        			itr.remove();
        			break;
        		}
        	}
        }	
    }
    
    @Override
    public void removeRemoteReadKey(Object key){
    	
    	if(remoteReadSet != null){
        	Iterator<ReadSetEntry> itr = remoteReadSet.descendingIterator();
        	while(itr.hasNext()){
        		ReadSetEntry entry = itr.next();
        		
        		if(entry.getKey() != null && entry.getKey().equals(key)){
        			itr.remove();
        			break;
        		}
        	}
        }
    }
    
    @Override
    public void addRemoteReadKey(Object key, InternalMVCCEntry ime) {
        if(remoteReadSet == null) {
            remoteReadSet = new LinkedList<ReadSetEntry>();
        }
        remoteReadSet.addLast(new ReadSetEntry(key, ime));
    }
    
    
	public void setLastReadKey(CacheEntry entry){
		this.lastReadKey = entry;
	}

	
	public CacheEntry getLastReadKey(){
		return this.lastReadKey;
	}

	
	public void clearLastReadKey(){
		this.lastReadKey = null;
	}

    public void setCommitVersion(VersionVC version) {
        this.commitVersion = version;
    }

    public VersionVC getCommitVersion() {
        return commitVersion;
    }

    public boolean isLocallyValidated(){
        return this.locallyValidated;
    }

    public void markLocallyValidated(){
        this.locallyValidated = true;
    }
}
