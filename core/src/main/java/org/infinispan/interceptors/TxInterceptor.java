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
package org.infinispan.interceptors;

import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.commands.write.*;
import org.infinispan.config.Configuration;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.LocalTransaction;
import org.infinispan.transaction.TransactionCoordinator;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.rhq.helpers.pluginAnnotations.agent.*;

import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.Transaction;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Interceptor in charge with handling transaction related operations, e.g enlisting cache as an transaction
 * participant, propagating remotely initiated changes.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @author Mircea.Markus@jboss.com
 * @see org.infinispan.transaction.xa.TransactionXaAdapter
 * @since 4.0
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class TxInterceptor extends CommandInterceptor {

    private TransactionLog transactionLog;
    private TransactionTable txTable;
    protected TransactionCoordinator txCoordinator;

    //stats
    private final AtomicLong prepares = new AtomicLong(0);
    private final AtomicLong commits = new AtomicLong(0);
    private final AtomicLong rollbacks = new AtomicLong(0);
    //this is only for remote commands only!!
    private final AtomicLong successPrepareTime = new AtomicLong(0);
    private final AtomicLong failedPrepareTime = new AtomicLong(0);
    private final AtomicLong nrSuccessPrepare = new AtomicLong(0);
    private final AtomicLong nrFailedPrepare = new AtomicLong(0);
    private final AtomicLong remoteCommitTime = new AtomicLong(0);
    private final AtomicLong nrRemoteCommit = new AtomicLong(0);
    private final AtomicLong rollbackTime = new AtomicLong(0);
    private final AtomicLong nrRollback = new AtomicLong(0);
    //this is only for local commands
    private final AtomicLong rollbacksDueToUnableAcquireLock = new AtomicLong(0);
    private final AtomicLong rollbacksDueToDeadLock = new AtomicLong(0);
    //read operation
    private final AtomicLong readTime = new AtomicLong(0);
    private final AtomicLong remoteReadTime = new AtomicLong(0);
    private final AtomicLong nrReadOp = new AtomicLong(0);
    private final AtomicLong nrRemoteReadOp = new AtomicLong(0);
    
    private final AtomicLong localCommitTime = new AtomicLong(0);
    private final AtomicLong nrLocalCommit = new AtomicLong(0);


    @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
    protected boolean statisticsEnabled;


    @Inject
    public void init(TransactionTable txTable, TransactionLog transactionLog, Configuration c, TransactionCoordinator txCoordinator) {
        this.configuration = c;
        this.transactionLog = transactionLog;
        this.txTable = txTable;
        this.txCoordinator = txCoordinator;
        setStatisticsEnabled(configuration.isExposeJmxStatistics());
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        boolean successful = true;
        long start = System.nanoTime();
        try {
            if (!ctx.isOriginLocal()) {
                // replay modifications
                for (VisitableCommand modification : command.getModifications()) {
                    VisitableCommand toReplay = getCommandToReplay(modification);
                    if (toReplay != null) {
                        try {
                            invokeNextInterceptor(ctx, toReplay);
                        } catch (Exception e) {
                            // If exception encountered, i.e. DeadlockDetectedException
                            // in an async env (i.e. isOnePhaseCommit()), clear the
                            // remote transaction, otherwise it leaks
                            if (command.isOnePhaseCommit())
                                markCompleted(ctx, command.getGlobalTransaction(), false);

                            // Now rethrow the original exception
                            throw e;
                        }
                    }
                }
            }
            //if it is remote and 2PC then first log the tx only after replying mods
            if (!command.isOnePhaseCommit()) {
                transactionLog.logPrepare(command);
            }
            if (this.statisticsEnabled) prepares.incrementAndGet();




            Object result = invokeNextInterceptor(ctx, command);
            if (command.isOnePhaseCommit()) {
                transactionLog.logOnePhaseCommit(ctx.getGlobalTransaction(), command.getModifications());
            }
            if (!ctx.isOriginLocal()) {
                if (command.isOnePhaseCommit()) {
                    markCompleted(ctx, command.getGlobalTransaction(), true);
                } else {
                    txTable.remoteTransactionPrepared(command.getGlobalTransaction());
                }
            }

            return result;
        } catch(TimeoutException e) {
            successful = false;
            if(statisticsEnabled && ctx.isOriginLocal()) {
                rollbacksDueToUnableAcquireLock.incrementAndGet();
            }
            throw e;
        } catch(DeadlockDetectedException e) {
            successful = false;
            if(statisticsEnabled && ctx.isOriginLocal()) {
                rollbacksDueToDeadLock.incrementAndGet();
            }
            throw e;
        } finally {
            if(statisticsEnabled && !ctx.isOriginLocal()) {
                long end = System.nanoTime();
                if(successful) {
                    successPrepareTime.addAndGet(end - start);
                    nrSuccessPrepare.incrementAndGet();
                } else {
                    failedPrepareTime.addAndGet(end - start);
                    nrFailedPrepare.incrementAndGet();
                }
            }
        }
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        long start = System.nanoTime();
        try {
            if (this.statisticsEnabled) {
                commits.incrementAndGet();
            }
            Object result = invokeNextInterceptor(ctx, command);
            markCompleted(ctx, command.getGlobalTransaction(), true);
            transactionLog.logCommit(command.getGlobalTransaction());
            return result;
        } finally {
            if(statisticsEnabled) {
                long end = System.nanoTime();
                if(!ctx.isOriginLocal()){
                remoteCommitTime.addAndGet(end - start);
                nrRemoteCommit.incrementAndGet();
                }
                else{
                	localCommitTime.addAndGet(end - start);
                	nrLocalCommit.incrementAndGet();
                }
            }
        }
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        long start = System.nanoTime();
        try {
            if (this.statisticsEnabled) {
                rollbacks.incrementAndGet();
            }
            transactionLog.rollback(command.getGlobalTransaction());
            markCompleted(ctx, command.getGlobalTransaction(), false);
            return invokeNextInterceptor(ctx, command);
        } finally {
            if(statisticsEnabled && !ctx.isOriginLocal()) {
                long end = System.nanoTime();
                rollbackTime.addAndGet(end - start);
                nrRollback.incrementAndGet();
            }
        }
    }

    @Override
    public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
        return enlistReadAndInvokeNext(ctx, command);
    }

    @Override
    public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
        return enlistWriteAndInvokeNext(ctx, command);
    }

    @Override
    public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
        return enlistWriteAndInvokeNext(ctx, command);
    }

    @Override
    public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
        return enlistWriteAndInvokeNext(ctx, command);
    }

    @Override
    public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
        return enlistWriteAndInvokeNext(ctx, command);
    }

    @Override
    public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
        return enlistWriteAndInvokeNext(ctx, command);
    }

    @Override
    public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
        return enlistWriteAndInvokeNext(ctx, invalidateCommand);
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        long start = System.nanoTime();
        try {
            return enlistReadAndInvokeNext(ctx, command);
        } finally {
            if(statisticsEnabled) {
                long end = System.nanoTime();
                if(ctx.isOriginLocal()) {
                    readTime.addAndGet(end - start);
                    nrReadOp.incrementAndGet();
                } else {
                    remoteReadTime.addAndGet(end - start);
                    nrRemoteReadOp.incrementAndGet();
                }
            }
        }
    }

    protected Object enlistReadAndInvokeNext(InvocationContext ctx, VisitableCommand command) throws Throwable {
        if (shouldEnlist(ctx)) {
            LocalTransaction localTransaction = enlist(ctx);
            LocalTxInvocationContext localTxContext = (LocalTxInvocationContext) ctx;
            localTxContext.setLocalTransaction(localTransaction);
        }
        return invokeNextInterceptor(ctx, command);
    }

    private Object enlistWriteAndInvokeNext(InvocationContext ctx, WriteCommand command) throws Throwable {
        LocalTransaction localTransaction = null;
        boolean shouldAddMod = false;
        if (shouldEnlist(ctx)) {
            localTransaction = enlist(ctx);
            LocalTxInvocationContext localTxContext = (LocalTxInvocationContext) ctx;
            if (localModeNotForced(ctx)) shouldAddMod = true;
            localTxContext.setLocalTransaction(localTransaction);
        }
        Object rv;
        rv = invokeNextInterceptor(ctx, command);
        if (!ctx.isInTxScope())
            transactionLog.logNoTxWrite(command);
        if (command.isSuccessful() && shouldAddMod) localTransaction.addModification(command);
        return rv;
    }

    public LocalTransaction enlist(InvocationContext ctx) throws SystemException, RollbackException {
        Transaction transaction = ((TxInvocationContext) ctx).getTransaction();
        if (transaction == null) throw new IllegalStateException("This should only be called in an tx scope");
        int status = transaction.getStatus();
        if (isNotValid(status)) throw new IllegalStateException("Transaction " + transaction +
                " is not in a valid state to be invoking cache operations on.");
        LocalTransaction localTransaction = txTable.getOrCreateLocalTransaction(transaction, ctx);
        txTable.enlist(transaction, localTransaction);
        return localTransaction;
    }

    private boolean isNotValid(int status) {
        return status != Status.STATUS_ACTIVE && status != Status.STATUS_PREPARING;
    }

    private boolean shouldEnlist(InvocationContext ctx) {
        return ctx.isInTxScope() && ctx.isOriginLocal();
    }

    private boolean localModeNotForced(InvocationContext icx) {
        if (icx.hasFlag(Flag.CACHE_MODE_LOCAL)) {
            if (trace) log.debug("LOCAL mode forced on invocation.  Suppressing clustered events.");
            return false;
        }
        return true;
    }

    private void markCompleted(TxInvocationContext ctx, GlobalTransaction globalTransaction, boolean committed) {
        if (!ctx.isOriginLocal()) txTable.remoteTransactionCompleted(globalTransaction, committed);
    }

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics")
    public void resetStatistics() {
        prepares.set(0);
        commits.set(0);
        rollbacks.set(0);
        successPrepareTime.set(0);
        failedPrepareTime.set(0);
        nrSuccessPrepare.set(0);
        nrFailedPrepare.set(0);
        remoteCommitTime.set(0);
        nrRemoteCommit.set(0);
        rollbackTime.set(0);
        nrRollback.set(0);
        rollbacksDueToUnableAcquireLock.set(0);
        rollbacksDueToDeadLock.set(0);
        readTime.set(0);
        remoteReadTime.set(0);
        nrReadOp.set(0);
        nrRemoteReadOp.set(0);
        localCommitTime.set(0);
    	nrLocalCommit.set(0);
    }

    @Operation(displayName = "Enable/disable statistics")
    public void setStatisticsEnabled(@Parameter(name = "enabled", description = "Whether statistics should be enabled or disabled (true/false)") boolean enabled) {
        this.statisticsEnabled = enabled;
    }

    @Metric(displayName = "Statistics enabled", dataType = DataType.TRAIT)
    public boolean isStatisticsEnabled() {
        return this.statisticsEnabled;
    }

    @ManagedAttribute(description = "Number of transaction prepares performed since last reset")
    @Metric(displayName = "Prepares", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getPrepares() {
        return prepares.get();
    }

    @ManagedAttribute(description = "Number of transaction commits performed since last reset")
    @Metric(displayName = "Commits", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getCommits() {
        return commits.get();
    }

    @ManagedAttribute(description = "Number of transaction rollbacks performed since last reset")
    @Metric(displayName = "Rollbacks", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRollbacks() {
        return rollbacks.get();
    }

    @ManagedAttribute(description = "Duration of all successful remote prepare command since last reset (nano-seconds)")
    @Metric(displayName = "SuccessPrepareTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getSuccessPrepareTime() {
        return successPrepareTime.get();
    }

    @ManagedAttribute(description = "Duration of all failed remote prepare command since last reset (nano-seconds)")
    @Metric(displayName = "FailedPrepareTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getFailedPrepareTime() {
        return failedPrepareTime.get();
    }

    @ManagedAttribute(description = "Number of successful remote prepare command performed since last reset")
    @Metric(displayName = "NrSuccessPrepare", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrSuccessPrepare() {
        return nrSuccessPrepare.get();
    }

    @ManagedAttribute(description = "Number of failed remote prepare command performed since last reset")
    @Metric(displayName = "NrFailedPrepare", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrFailedPrepare() {
        return nrFailedPrepare.get();
    }

    @ManagedAttribute(description = "Duration of all remote commit command since last reset (nano-seconds)")
    @Metric(displayName = "RemoteCommitTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRemoteCommitTime() {
        return remoteCommitTime.get();
    }

    @ManagedAttribute(description = "Number of remote commit command performed since last reset")
    @Metric(displayName = "NrRemoteCommit", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrRemoteCommit() {
        return nrRemoteCommit.get();
    }
    
    @ManagedAttribute(description = "Duration of all local commit command since last reset (nano-seconds)")
    @Metric(displayName = "LocalCommitTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getLocalCommitTime() {
        return localCommitTime.get();
    }

    @ManagedAttribute(description = "Number of local commit command performed since last reset")
    @Metric(displayName = "NrLocalCommit", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrLocalCommit() {
        return nrLocalCommit.get();
    }

    @ManagedAttribute(description = "Duration of all remote rollback command since last reset (nano-seconds)")
    @Metric(displayName = "RollbackTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRollbackTime() {
        return rollbackTime.get();
    }

    @ManagedAttribute(description = "Number of remote rollback command performed since last reset")
    @Metric(displayName = "NrRollback", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrRollback() {
        return nrRollback.get();
    }

    @ManagedAttribute(description = "Number of rollbacks due to unable of acquire the locks since last reset")
    @Metric(displayName = "RollbacksDueToUnableAcquireLock", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRollbacksDueToUnableAcquireLock() {
        return rollbacksDueToUnableAcquireLock.get();
    }

    @ManagedAttribute(description = "Number of rollbacks due to dead locks since last reset")
    @Metric(displayName = "RollbacksDueToDeadLock", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRollbacksDueToDeadLock() {
        return rollbacksDueToDeadLock.get();
    }

    @ManagedAttribute(description = "Number of rollbacks due to validation failed since last reset")
    @Metric(displayName = "RollbacksDueToValidation", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRollbacksDueToValidation() {
        return 0;
    }

    @ManagedAttribute(description = "Duration of all read command since last reset (nano-seconds)")
    @Metric(displayName = "ReadTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getReadTime() {
        return readTime.get();
    }

    @ManagedAttribute(description = "Duration of all remote read command since last reset (nano-seconds)")
    @Metric(displayName = "RemoteReadTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRemoteReadTime() {
        return remoteReadTime.get();
    }

    @ManagedAttribute(description = "Number of read commands since last reset")
    @Metric(displayName = "NrReadOp", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrReadOp() {
        return nrReadOp.get();
    }

    @ManagedAttribute(description = "Number of remote read commands since last reset")
    @Metric(displayName = "NrRemoteReadOp", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrRemoteReadOp() {
        return nrRemoteReadOp.get();
    }
    
    @ManagedAttribute(description = "Number of successful read-write locks acquisitions")
    @Metric(displayName = "NrRWLocksAcquisitions", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrRWLocksAcquisitions() {
        return 0L;
    }
    
    @ManagedAttribute(description = "Total duration of successful read-write locks acquisitions since last reset (nanoseconds)")
    @Metric(displayName = "RWLocksAcquisitionTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRWLocksAcquisitionTime() {
        return 0L;
    }

    /**
     * Designed to be overridden.  Returns a VisitableCommand fit for replaying locally, based on the modification passed
     * in.  If a null value is returned, this means that the command should not be replayed.
     *
     * @param modification modification in a prepare call
     * @return a VisitableCommand representing this modification, fit for replaying, or null if the command should not be
     *         replayed.
     */
    protected VisitableCommand getCommandToReplay(VisitableCommand modification) {
        return modification;
    }

    @Override
    public Object visitTotalOrderPrepareCommand(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        if (!ctx.isOriginLocal()) {
            // replay modifications
            for (VisitableCommand modification : command.getModifications()) {
                VisitableCommand toReplay = getCommandToReplay(modification);
                if (toReplay != null) {
                    try {
                        invokeNextInterceptor(ctx, toReplay);
                    } catch (Exception e) {
                        // If exception encountered, i.e. DeadlockDetectedException
                        // in an async env (i.e. isOnePhaseCommit()), clear the
                        // remote transaction, otherwise it leaks
                        if (command.isOnePhaseCommit())
                            markCompleted(ctx, command.getGlobalTransaction(), false);

                        // Now rethrow the original exception
                        throw e;
                    }
                }
            }
        }
        //if it is remote and 2PC then first log the tx only after replying mods
        if (!command.isOnePhaseCommit()) {
            transactionLog.logPrepare(command);
        }
        if (this.statisticsEnabled) {
            prepares.incrementAndGet();
        }

        Object result = invokeNextInterceptor(ctx, command);

        if (command.isOnePhaseCommit()) {
            transactionLog.logOnePhaseCommit(ctx.getGlobalTransaction(), command.getModifications());
        }

        if (!ctx.isOriginLocal()) {
            if (command.isOnePhaseCommit()) {
                markCompleted(ctx, command.getGlobalTransaction(), true);
            } else {
                txTable.remoteTransactionPrepared(command.getGlobalTransaction());
            }
        }
        return result;
    }
}
