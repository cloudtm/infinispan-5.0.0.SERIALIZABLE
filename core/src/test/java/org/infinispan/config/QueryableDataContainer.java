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
package org.infinispan.config;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static java.util.Collections.synchronizedCollection;

public class QueryableDataContainer implements DataContainer {

    private static DataContainer delegate;

    public static void setDelegate(DataContainer delegate) {
        QueryableDataContainer.delegate = delegate;
    }

    private final Collection<String> loggedOperations;

    public void setFoo(String foo) {
        loggedOperations.add("setFoo(" + foo + ")");
    }

    public QueryableDataContainer() {
        this.loggedOperations = synchronizedCollection(new ArrayList<String>());
    }

    @Override
    public Iterator<InternalCacheEntry> iterator() {
        loggedOperations.add("iterator()");
        return delegate.iterator();
    }

    @Override
    public InternalCacheEntry get(Object k) {
        loggedOperations.add("get(" + k + ")" );
        return delegate.get(k);
    }

    @Override
    public InternalCacheEntry peek(Object k) {
        loggedOperations.add("peek(" + k + ")" );
        return delegate.peek(k);
    }

    @Override
    public void put(Object k, Object v, long lifespan, long maxIdle) {
        loggedOperations.add("put(" + k + ", " + v + ", " + lifespan + ", " + maxIdle + ")");
        delegate.put(k, v, lifespan, maxIdle);
    }

    @Override
    public boolean containsKey(Object k) {
        loggedOperations.add("containsKey(" + k + ")" );
        return delegate.containsKey(k);
    }

    @Override
    public InternalCacheEntry remove(Object k) {
        loggedOperations.add("remove(" + k + ")" );
        return delegate.remove(k);
    }

    @Override
    public int size() {
        loggedOperations.add("size()" );
        return delegate.size();
    }

    @Override
    public void clear() {
        loggedOperations.add("clear()" );
        delegate.clear();
    }

    @Override
    public Set<Object> keySet() {
        loggedOperations.add("keySet()" );
        return delegate.keySet();
    }

    @Override
    public Collection<Object> values() {
        loggedOperations.add("values()" );
        return delegate.values();
    }

    @Override
    public Set<InternalCacheEntry> entrySet() {
        loggedOperations.add("entrySet()" );
        return delegate.entrySet();
    }

    @Override
    public void purgeExpired() {
        loggedOperations.add("purgeExpired()" );
        delegate.purgeExpired();
    }

    public Collection<String> getLoggedOperations() {
        return loggedOperations;
    }

    /*
     * =========== added by Pedro =============
     */

    @Override
    public InternalMVCCEntry get(Object k, VersionVC max, boolean firstTimeOnNode) {
        loggedOperations.add("get(" + k + ", " + max + ")");
        return delegate.get(k, max, firstTimeOnNode);
    }

    @Override
    public InternalMVCCEntry peek(Object k, VersionVC max, boolean firstTimeOnNode) {
        loggedOperations.add("peek(" + k + ", " + max + ")");
        return delegate.peek(k, max, firstTimeOnNode);
    }

    @Override
    public void put(Object k, Object v, long lifespan, long maxIdle, VersionVC version) {
        loggedOperations.add("put(" + k + ", " + v + ", " + lifespan + ", " + maxIdle + ", " + version + ")");
        delegate.put(k, v, lifespan, maxIdle, version);
    }

    @Override
    public boolean containsKey(Object k, VersionVC max, boolean firstTimeOnNode) {
        loggedOperations.add("containsKey(" + k + ", " + max + ")" );
        return delegate.containsKey(k,max, firstTimeOnNode);
    }

    @Override
    public InternalCacheEntry remove(Object k, VersionVC version) {
        loggedOperations.add("remove(" + k + ", " + version + ")" );
        return delegate.remove(k, version);
    }

    @Override
    public int size(VersionVC max, boolean firstTimeOnNode) {
        loggedOperations.add("size(" + max + ")" );
        return delegate.size(max, firstTimeOnNode);
    }

    @Override
    public void clear(VersionVC version) {
        loggedOperations.add("clear(" + version + ")" );
        delegate.clear(version);
    }

    @Override
    public Set<Object> keySet(VersionVC max, boolean firstTimeOnNode) {
        loggedOperations.add("keySet(" + max + ")" );
        return delegate.keySet(max, firstTimeOnNode);
    }

    @Override
    public Collection<Object> values(VersionVC max, boolean firstTimeOnNode) {
        loggedOperations.add("values(" + max + ")" );
        return delegate.values(max, firstTimeOnNode);
    }

    @Override
    public Set<InternalCacheEntry> entrySet(VersionVC max) {
        loggedOperations.add("entrySet(" + max + ")" );
        return delegate.entrySet(max);
    }

    @Override
    public void purgeExpired(VersionVC version, boolean firstTimeOnNode) {
        loggedOperations.add("purgeExpired(" + version + ")" );
        delegate.purgeExpired(version, firstTimeOnNode);
    }

    @Override
    public boolean validateKey(Object key, VersionVC version) {
        loggedOperations.add("validateKey(" + key + "," + version + ")" );
        return delegate.validateKey(key, version);
    }
}
