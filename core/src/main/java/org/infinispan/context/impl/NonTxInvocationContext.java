/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
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
package org.infinispan.context.impl;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.ReadSetEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.BidirectionalMap;
import org.infinispan.util.InfinispanCollections;

import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

/**
 * Context to be used for non transactional calls, both remote and local.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class NonTxInvocationContext extends AbstractInvocationContext {

    protected BidirectionalLinkedHashMap<Object, CacheEntry> lookedUpEntries = null;

    //for serializability
    private boolean readBasedOnVersion = false;
    private VersionVC versionVC = null;
    
    protected Deque<ReadSetEntry> readKeys = new LinkedList<ReadSetEntry>();
    
    private CacheEntry lastReadKey = null;
    
    private boolean alreadyReadOnNode = false;

    public CacheEntry lookupEntry(Object k) {
        return lookedUpEntries == null ? null : lookedUpEntries.get(k);
    }

    public void removeLookedUpEntry(Object key) {
        if (lookedUpEntries != null) lookedUpEntries.remove(key);
    }

    public void putLookedUpEntry(Object key, CacheEntry e) {
        initLookedUpEntries();
        lookedUpEntries.put(key, e);
    }

    public void putLookedUpEntries(Map<Object, CacheEntry> lookedUpEntries) {
        initLookedUpEntries();
        for (Map.Entry<Object, CacheEntry> ce: lookedUpEntries.entrySet()) {
            lookedUpEntries.put(ce.getKey(), ce.getValue());
        }
    }

    public void clearLookedUpEntries() {
        if (lookedUpEntries != null) lookedUpEntries.clear();
    }

    @SuppressWarnings("unchecked")
    public BidirectionalMap<Object, CacheEntry> getLookedUpEntries() {
        return (BidirectionalMap<Object, CacheEntry>)
                (lookedUpEntries == null ? InfinispanCollections.emptyBidirectionalMap() : lookedUpEntries);
    }

    public boolean isOriginLocal() {
        return isContextFlagSet(ContextFlag.ORIGIN_LOCAL);
    }

    public void setOriginLocal(boolean originLocal) {
        setContextFlag(ContextFlag.ORIGIN_LOCAL, originLocal);
    }

    public boolean isInTxScope() {
        return false;
    }

    public Object getLockOwner() {
        return Thread.currentThread();
    }

    private void initLookedUpEntries() {
        if (lookedUpEntries == null) lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(4);
    }

    @Override
    public void reset() {
        super.reset();
        clearLookedUpEntries();
        resetMultiVersion();
    }

    @Override
    public NonTxInvocationContext clone() {
        NonTxInvocationContext dolly = (NonTxInvocationContext) super.clone();
        if (lookedUpEntries != null) {
            dolly.lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(lookedUpEntries);
        }
        if(readKeys != null) {
            dolly.readKeys = new LinkedList<ReadSetEntry>(readKeys);
        }
        dolly.readBasedOnVersion = readBasedOnVersion;
        dolly.versionVC = versionVC;
        return dolly;
    }

    @Override
    public boolean readBasedOnVersion() {
        return readBasedOnVersion;
    }

    @Override
    public void setReadBasedOnVersion(boolean value) {
        readBasedOnVersion = value;
    }

    @Override
    public void setVersionToRead(VersionVC version) {
        versionVC = version;
    }
    
    @Override
    public void addLocalReadKey(Object key, InternalMVCCEntry ime) {
        readKeys.addLast(new ReadSetEntry(key, ime));
    }
    
    @Override
    public void removeLocalReadKey(Object key) {
    	if(readKeys != null){
        	Iterator<ReadSetEntry> itr = readKeys.descendingIterator();
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
    public void removeRemoteReadKey(Object key) {
    	
    	//no-op
    }
    
    @Override
    public void addRemoteReadKey(Object key, InternalMVCCEntry ime) {
        //no-op
    }

    @Override
    public InternalMVCCEntry getLocalReadKey(Object key) {
    	return readKeys == null ? null : find(readKeys, key);
    }
    
    private InternalMVCCEntry find(Deque<ReadSetEntry> d, Object key){
    	if(d != null){
        	Iterator<ReadSetEntry> itr = d.descendingIterator();
        	while(itr.hasNext()){
        		ReadSetEntry entry = itr.next();
        		
        		if(entry.getKey() != null && entry.getKey().equals(key)){
        			return entry.getIme();
        		}
        	}
        }
    	
    	return null;
    }
    
    @Override
    public InternalMVCCEntry getRemoteReadKey(Object key) {
        return null;
    }
    

    @Override
    public VersionVC calculateVersionToRead(VersionVCFactory versionVCFactory) {
        return versionVC;
    }
    
    @Override
    public void setAlreadyReadOnNode(boolean alreadyRead){
    	this.alreadyReadOnNode = alreadyRead;
    }
    
    @Override
    public boolean getAlreadyReadOnNode(){
    	return this.alreadyReadOnNode;
    }

    private void resetMultiVersion() {
        readBasedOnVersion = false;
        versionVC = null;
        readKeys.clear();
    }
    
    @Override
    public void setLastReadKey(CacheEntry entry){
    	this.lastReadKey = entry;
    }
    
    @Override
    public CacheEntry getLastReadKey(){
    	return this.lastReadKey;
    }
    
    @Override
    public void clearLastReadKey(){
    	this.lastReadKey = null;
    }
}
