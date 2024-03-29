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
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.InvalidTransactionException;
import org.infinispan.util.BidirectionalLinkedHashMap;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

/**
 * Defines the state of a remotely originated transaction.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class RemoteTransaction extends AbstractCacheTransaction implements Cloneable {

    private static final Log log = LogFactory.getLog(RemoteTransaction.class);

    private volatile boolean valid = true;

    //Sebastiano
    private CountDownLatch prepareCountDown;

    public RemoteTransaction(WriteCommand[] modifications, GlobalTransaction tx) {
        this.modifications = modifications == null || modifications.length == 0 ? Collections.<WriteCommand>emptyList() : Arrays.asList(modifications);
        lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>(this.modifications.size());
        this.tx = tx;
        this.prepareCountDown = new CountDownLatch(1);
    }

    public RemoteTransaction(GlobalTransaction tx) {
        this.modifications = new LinkedList<WriteCommand>();
        lookedUpEntries = new BidirectionalLinkedHashMap<Object, CacheEntry>();
        this.tx = tx;
        this.prepareCountDown = new CountDownLatch(1);
    }

    public void invalidate() {
        valid = false;
    }



    public void putLookedUpEntry(Object key, CacheEntry e) {
        if (valid) {
            if (log.isTraceEnabled()) {
                log.tracef("Adding key %s to tx %s", key, getGlobalTransaction());
            }
            lookedUpEntries.put(key, e);
        } else {
            throw new InvalidTransactionException("This remote transaction " + getGlobalTransaction() + " is invalid");
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RemoteTransaction)) return false;
        RemoteTransaction that = (RemoteTransaction) o;
        return tx.equals(that.tx);
    }

    @Override
    public int hashCode() {
        return tx.hashCode();
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object clone() {
        try {
            RemoteTransaction dolly = (RemoteTransaction) super.clone();
            dolly.modifications = new ArrayList<WriteCommand>(modifications);
            dolly.lookedUpEntries = lookedUpEntries.clone();
            return dolly;
        } catch (CloneNotSupportedException e) {
            throw new IllegalStateException("Impossible!!");
        }
    }

    @Override
    public String toString() {
        return "RemoteTransaction{" +
                "modifications=" + modifications +
                ", lookedUpEntries=" + lookedUpEntries +
                ", tx=" + tx +
                '}';
    }

    public Set<Object> getLockedKeys() {
        Set<Object> result = new HashSet<Object>();
        for (Object key : getLookedUpEntries().keySet()) {
            result.add(key);
        }
        if (lookedUpEntries.entrySet().size() != result.size())
            throw new IllegalStateException("Different sizes!");
        return result;
    }

    @Override
    public void addLocalReadKey(Object key, InternalMVCCEntry ime) {
        //no-op
    }
    
    @Override
    public void removeLocalReadKey(Object key) {
        //no-op
    
    }
    
    @Override
    public void removeRemoteReadKey(Object key){
    	//no-op
    }
    
    @Override
    public void addRemoteReadKey(Object key, InternalMVCCEntry ime) {
        //no-op
    }

    //Sebastiano
    public void waitForPrepare() {

        boolean prepared = false;

        while(!prepared){
            try {
                this.prepareCountDown.await();
                prepared = true;

            } catch (InterruptedException e) {
                prepared = false;
            }
        }

    }

    //Sebastiano
    public void notifyEndPrepare() {

        this.prepareCountDown.countDown();

    }
}
