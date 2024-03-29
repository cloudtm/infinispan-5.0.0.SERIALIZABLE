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
package org.infinispan.transaction.xa;

import org.infinispan.CacheException;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.Transaction;
import javax.transaction.xa.XAException;
import javax.transaction.xa.Xid;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * {@link TransactionTable} to be used with {@link TransactionXaAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class XaTransactionTable extends TransactionTable {

    private static final Log log = LogFactory.getLog(XaTransactionTable.class);

    protected final ConcurrentMap<Xid, LocalXaTransaction> xid2LocalTx = new ConcurrentHashMap<Xid, LocalXaTransaction>();
    private RecoveryManager recoveryManager;
    
    //added by Sebastiano
    private VersionVCFactory versionVCFactory;

    @Inject
    public void init(RecoveryManager recoveryManager, VersionVCFactory versionVCFactory) {
        this.recoveryManager = recoveryManager;
        this.versionVCFactory=versionVCFactory;
    }

    @Override
    public boolean removeLocalTransaction(LocalTransaction localTx) {
        boolean result = false;
        if (localTx.getTransaction() != null) {//this can be null when we force the invocation during recovery, perhaps on a remote node
            result = super.removeLocalTransaction(localTx);
        }
        LocalXaTransaction xaLocalTransaction = (LocalXaTransaction) localTx;
        xid2LocalTx.remove(xaLocalTransaction.getXid());
        return result;
    }

    public LocalXaTransaction getLocalTransaction(Xid xid) {
        return this.xid2LocalTx.get(xid);
    }

    public void addLocalTransactionMapping(LocalXaTransaction localTransaction) {
        if (localTransaction.getXid() == null) throw new IllegalStateException("Initialize xid first!");
        this.xid2LocalTx.put(localTransaction.getXid(), localTransaction);
    }

    @Override
    public void enlist(Transaction transaction, LocalTransaction ltx) {
        LocalXaTransaction localTransaction = (LocalXaTransaction) ltx;
        if (!localTransaction.isEnlisted()) { //make sure that you only enlist it once
            try {
                transaction.enlistResource(new TransactionXaAdapter(localTransaction, this, configuration, recoveryManager, txCoordinator));
            } catch (Exception e) {
                Xid xid = localTransaction.getXid();
                if (xid != null && !localTransaction.getLookedUpEntries().isEmpty()) {
                    log.debug("Attempting a rollback to clear stale resources!");
                    try {
                        txCoordinator.rollback(localTransaction);
                    } catch (XAException xae) {
                        log.debug("Caught exception attempting to clean up " + xid, xae);
                    }
                }
                log.error("Failed to enlist TransactionXaAdapter to transaction", e);
                throw new CacheException(e);
            }
            //initiates the vector clock for serializability
            localTransaction.initVectorClock(this.versionVCFactory, commitLog.getActualVersion());
        }
    }

    public RecoveryManager getRecoveryManager() {
        return recoveryManager;
    }

    public void setRecoveryManager(RecoveryManager recoveryManager) {
        this.recoveryManager = recoveryManager;
    }

    @Override
    public int getLocalTxCount() {
        return xid2LocalTx.size();
    }
}
