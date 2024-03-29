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

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import javax.transaction.xa.XAException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static javax.transaction.xa.XAResource.XA_OK;
import static javax.transaction.xa.XAResource.XA_RDONLY;

/**
 * Coordinates transaction prepare/commits as received from the {@link javax.transaction.TransactionManager}.
 * Integrates with the TM through either {@link org.infinispan.transaction.xa.TransactionXaAdapter} or
 * through {@link org.infinispan.transaction.synchronization.SynchronizationAdapter}.
 *
 * @author Mircea.Markus@jboss.com
 * @since 5.0
 */
public class TransactionCoordinator {

    private static final Log log = LogFactory.getLog(TransactionCoordinator.class);
    private CommandsFactory commandsFactory;
    private InvocationContextContainer icc;
    private InterceptorChain invoker;
    private TransactionTable txTable;
    private Configuration configuration;
    private RpcManager rpcManager;
    private boolean RRwithWriteSkew;
    private boolean isTotalOrderReplication, useSerializable;

    boolean trace;

    @Inject
    public void init(CommandsFactory commandsFactory, InvocationContextContainer icc, InterceptorChain invoker,
                     TransactionTable txTable, Configuration configuration, RpcManager rpcManager) {
        this.commandsFactory = commandsFactory;
        this.icc = icc;
        this.invoker = invoker;
        this.txTable = txTable;
        this.configuration = configuration;
        trace = log.isTraceEnabled();
        this.rpcManager = rpcManager;
        RRwithWriteSkew = configuration.isWriteSkewCheck() && configuration.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
        isTotalOrderReplication = configuration.isTotalOrderReplication();
        useSerializable = configuration.getIsolationLevel() == IsolationLevel.SERIALIZABLE;
    }

    public int prepare(LocalTransaction localTransaction) throws XAException {
        validateNotMarkedForRollback(localTransaction);

        if (configuration.isOnePhaseCommit()) {
            if (trace) log.tracef("Received prepare for tx: %s. Skipping call as 1PC will be used.", localTransaction);
            return XA_OK;
        }

        if(isTotalOrderReplication &&
                (!RRwithWriteSkew || !remoteWriteSkewNeeded(localTransaction)) &&
                !useSerializable) {
            return XA_OK; //it is read committed in total order scheme... one phase needed
        }

        LocalTxInvocationContext ctx = icc.createTxInvocationContext();
        ctx.setLocalTransaction(localTransaction);

        PrepareCommand prepareCommand;
        if(isTotalOrderReplication) {
        	throw new RuntimeException("Problem: Total Order notsupported");
        	/*
            if(useSerializable) {
                TotalOrderPrepareCommand tOPrepareCommand = commandsFactory.buildTotalOrderPrepareCommand(localTransaction.getGlobalTransaction(),
                        localTransaction.getModifications(), localTransaction.getReadSet(),
                        ctx.getPrepareVersion(), configuration.isOnePhaseCommit());
                tOPrepareCommand.setOnePhaseCommit(false);
                tOPrepareCommand.setSerializability(true);
                prepareCommand = tOPrepareCommand;
            } else {
                TotalOrderPrepareCommand tOPrepareCommand = commandsFactory.buildTotalOrderPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications());
                tOPrepareCommand.setOnePhaseCommit(false); //we need a vote phase!
                prepareCommand = tOPrepareCommand;
            }
            */
        } else {
            if(useSerializable) {
            	if(configuration.getCacheMode().isDistributed()){
            		
            		prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(),
                            localTransaction.getModifications(), localTransaction.getRemoteReadSet(),
                            ctx.getPrepareVersion(), configuration.isOnePhaseCommit());
            	}
            	else{
            		prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(),
                        localTransaction.getModifications(), localTransaction.getLocalReadSet(),
                        ctx.getPrepareVersion(), configuration.isOnePhaseCommit());
            	}
            } else {
                prepareCommand = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), configuration.isOnePhaseCommit());
            }
        }
        if (trace) log.tracef("Sending prepare command through the chain: %s", prepareCommand);

        try {
            invoker.invoke(ctx, prepareCommand);
            if (localTransaction.isReadOnly()) {
                if (trace) log.tracef("Readonly transaction: %s", localTransaction.getGlobalTransaction());
                // force a cleanup to release any objects held.  Some TMs don't call commit if it is a READ ONLY tx.  See ISPN-845
                commit(localTransaction, false);
                return XA_RDONLY;
            } else {
                txTable.localTransactionPrepared(localTransaction);
                return XA_OK;
            }
        } catch (Throwable e) {
            log.error("Error while processing PrepareCommand", e);
            //rollback transaction before throwing the exception as there's no guarantee the TM calls XAResource.rollback
            //after prepare failed.
            rollback(localTransaction);
            throw new XAException(XAException.XAER_RMERR);
        }
    }

    public void commit(LocalTransaction localTransaction, boolean isOnePhase) throws XAException {
        if (trace) log.tracef("Committing transaction %s", localTransaction.getGlobalTransaction());
        try {
            LocalTxInvocationContext ctx = icc.createTxInvocationContext();
            ctx.setLocalTransaction(localTransaction);

            boolean totalOrderOnePhase = isTotalOrderReplication && (!RRwithWriteSkew || !remoteWriteSkewNeeded(localTransaction));

            if(totalOrderOnePhase && configuration.getIsolationLevel() != IsolationLevel.SERIALIZABLE) {
                validateNotMarkedForRollback(localTransaction);

                if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
                TotalOrderPrepareCommand command = commandsFactory.buildTotalOrderPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications());
                command.setOnePhaseCommit(true);
                try {
                    invoker.invoke(ctx, command);
                    txTable.removeLocalTransaction(localTransaction);
                } catch (Throwable e) {
                    txTable.failureCompletingTransaction(ctx.getTransaction());
                    log.errorProcessing1pcPrepareCommand(e);
                    throw new XAException(XAException.XAER_RMERR);
                }
            } else if (configuration.isOnePhaseCommit() || isOnePhase) {
                validateNotMarkedForRollback(localTransaction);

                if (trace) log.trace("Doing an 1PC prepare call on the interceptor chain");
                PrepareCommand command = commandsFactory.buildPrepareCommand(localTransaction.getGlobalTransaction(), localTransaction.getModifications(), true);
                try {
                    invoker.invoke(ctx, command);
                    txTable.removeLocalTransaction(localTransaction);
                } catch (Throwable e) {
                    txTable.failureCompletingTransaction(ctx.getTransaction());
                    log.errorProcessing1pcPrepareCommand(e);
                    throw new XAException(XAException.XAER_RMERR);
                }
            } else {
                handleTopologyChanges(localTransaction);
                CommitCommand commitCommand;
                if(useSerializable) {
                    commitCommand = commandsFactory.buildCommitCommand(localTransaction.getGlobalTransaction(),
                            localTransaction.getCommitVersion());
                } else {
                    commitCommand = commandsFactory.buildCommitCommand(localTransaction.getGlobalTransaction());
                }

                try {
                    invoker.invoke(ctx, commitCommand);
                    txTable.removeLocalTransaction(localTransaction);
                } catch (Throwable e) {
                    txTable.failureCompletingTransaction(ctx.getTransaction());
                    log.errorProcessing1pcPrepareCommand(e);
                    throw new XAException(XAException.XAER_RMERR);
                }
            }
        } finally {
            icc.suspend();
        }
    }

    public void rollback(LocalTransaction localTransaction) throws XAException {
        if (trace) log.tracef("rollback transaction %s ", localTransaction.getGlobalTransaction());
        RollbackCommand rollbackCommand = commandsFactory.buildRollbackCommand(localTransaction.getGlobalTransaction());
        LocalTxInvocationContext ctx = icc.createTxInvocationContext();
        ctx.setLocalTransaction(localTransaction);
        try {
            invoker.invoke(ctx, rollbackCommand);
            txTable.removeLocalTransaction(localTransaction);
        } catch (Throwable e) {
            txTable.failureCompletingTransaction(ctx.getTransaction());
            log.errorRollingBack(e);
            throw new XAException(XAException.XA_HEURHAZ);
        } finally {
            icc.suspend();
        }
    }

    /**
     */
    private void validateNotMarkedForRollback(LocalTransaction localTransaction) throws XAException {
        if (localTransaction.isMarkedForRollback()) {
            if (trace) log.tracef("Transaction already marked for rollback. Forcing rollback for %s", localTransaction);
            rollback(localTransaction);
            throw new XAException(XAException.XA_RBROLLBACK);
        }
    }

    /**
     * This method looks to see if some of the nodes present when transaction prepared are no longer in the cluster.
     * If this was the case it re-runs prepare.
     */
    private void handleTopologyChanges(LocalTransaction localTransaction) throws XAException {
        Collection<Address> preparedNodes = localTransaction.getRemoteLocksAcquired();
        List<Address> members = getClusterMembers();
        if (!members.containsAll(preparedNodes)) {
            if (trace) log.tracef("Member(s) left, so re-applying prepare for %s", localTransaction);
            localTransaction.filterRemoteLocksAcquire(members);
            try {
                prepare(localTransaction); //re-run prepare
            } catch (XAException e) {
                if (!configuration.isTransactionRecoveryEnabled()) {
                    rollback(localTransaction);
                    throw new XAException(XAException.XA_RBROLLBACK);
                }
            }
        } else if (trace) log.trace("All prepared nodes are okay.");
    }

    private List<Address> getClusterMembers() {
        return rpcManager != null ? rpcManager.getTransport().getMembers() : Collections.<Address>emptyList();
    }

    private boolean remoteWriteSkewNeeded(LocalTransaction localTx) {
        for(WriteCommand wc : localTx.getModifications()) {
            if(!wc.getKeyAndValuesForWriteSkewCheck().isEmpty()) {
                return true;
            }
        }
        return false;
    }
}
