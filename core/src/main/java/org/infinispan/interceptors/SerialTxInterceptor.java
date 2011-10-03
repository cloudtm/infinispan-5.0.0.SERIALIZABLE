package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.exception.ValidationException;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author pruivo
 *         Date: 11/08/11
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class SerialTxInterceptor extends TxInterceptor {
    private CommandsFactory commandsFactory;
    private CommitQueue commitQueue;
    private InvocationContextContainer icc;

    //this is only for remote commands only!!
    private final AtomicLong successPrepareTime = new AtomicLong(0);
    private final AtomicLong failedPrepareTime = new AtomicLong(0);
    private final AtomicLong nrSuccessPrepare = new AtomicLong(0);
    private final AtomicLong nrFailedPrepare = new AtomicLong(0);
    //this is only for local commands
    private final AtomicLong rollbacksDueToUnableAcquireLock = new AtomicLong(0);
    private final AtomicLong rollbacksDueToDeadLock = new AtomicLong(0);
    private final AtomicLong rollbacksDueToValidation = new AtomicLong(0);
    //read operation
    private final AtomicLong localReadTime = new AtomicLong(0);
    private final AtomicLong remoteReadTime = new AtomicLong(0);
    private final AtomicLong nrLocalReadOp = new AtomicLong(0);
    private final AtomicLong nrRemoteReadOp = new AtomicLong(0);

    @Inject
    public void inject(CommandsFactory commandsFactory, CommitQueue commitQueue, InvocationContextContainer icc) {
        this.commandsFactory = commandsFactory;
        this.commitQueue = commitQueue;
        this.icc = icc;
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        long start = System.nanoTime();
        boolean successful = true;
        try {
            Set<Object> writeSet = command.getAffectedKeys();
            Set<Object> readSet = command.getReadSet();

            if(writeSet != null && writeSet.isEmpty()) {
                log.debugf("new transaction [%s] arrived to SerialTxInterceptor. it is a Read-Only Transaction. returning",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
                return null;
            }

            log.debugf("new transaction [%s] arrived to SerialTxInterceptor. readSet=%s, writeSet=%s, version=%s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    readSet, writeSet, command.getVersion());

            //first acquire the read write locks and (second) validate the readset
            AcquireValidationLocksCommand  locksCommand = commandsFactory.buildAcquireValidationLocksCommand(
                    command.getGlobalTransaction(), readSet, writeSet, command.getVersion());
            invokeNextInterceptor(ctx, locksCommand);

            log.debugf("transaction [%s] passes validation",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));

            //third (this is equals to old schemes) wrap the wrote entries
            //finally, process the rest of the command
            Object retVal = super.visitPrepareCommand(ctx, command);

            VersionVC commitVC = commitQueue.addTransaction(command.getGlobalTransaction(), ctx.calculateVersionToRead(),
                    icc.getInvocationContext().clone(), 0);

            if(retVal != null && retVal instanceof VersionVC) {
                VersionVC othersCommitVC = (VersionVC) retVal;
                commitVC.setToMaximum(othersCommitVC);
            }
            ctx.setCommitVersion(commitVC);

            log.debugf("transaction [%s] commit vector clock is %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    commitVC);
            return commitVC;
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
        } catch(ValidationException e) {
            successful = false;
            if(statisticsEnabled && ctx.isOriginLocal()) {
                rollbacksDueToValidation.incrementAndGet();
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
        log.debugf("received commit command for %s", Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        if(ctx.isInTxScope() && ctx.isOriginLocal() && !ctx.hasModifications()) {
            log.debugf("try commit a read-only transaction [%s]. returning...",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            return null;
        }

        return super.visitCommitCommand(ctx, command);
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        log.debugf("received rollback command for %s", Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        return super.visitRollbackCommand(ctx, command);
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        long start = System.nanoTime();
        try {
            Object retval = super.visitGetKeyValueCommand(ctx, command);
            if(ctx.isInTxScope()) {
                ((TxInvocationContext) ctx).markReadFrom(0);
                //update vc
                InternalMVCCEntry ime = ctx.getReadKey(command.getKey());
                if(ime == null) {
                    log.warn("InternalMVCCEntry is null.");
                } else if(((TxInvocationContext) ctx).hasModifications() && !ime.isMostRecent()) {
                    //read an old value... the tx will abort in commit,
                    //so, do not waste time and abort it now
                    throw new CacheException("transaction must abort!! read an old value and it is not a read only transaction");
                } else {
                    VersionVC v = ime.getVersion();
                    if(v.get(0) == VersionVC.EMPTY_POSITION) {
                        v.set(0,0);
                    }
                    ((TxInvocationContext) ctx).updateVectorClock(v);
                }
            }

            return retval;
        } finally {
            if(statisticsEnabled) {
                long end = System.nanoTime();
                if(ctx.isOriginLocal()) {
                    localReadTime.addAndGet(end - start);
                    nrLocalReadOp.incrementAndGet();
                } else {
                    remoteReadTime.addAndGet(end - start);
                    nrRemoteReadOp.incrementAndGet();
                }
            }
        }
    }

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics")
    public void resetStatistics() {
        super.resetStatistics();
        successPrepareTime.set(0);
        failedPrepareTime.set(0);
        nrSuccessPrepare.set(0);
        nrFailedPrepare.set(0);
        rollbacksDueToUnableAcquireLock.set(0);
        rollbacksDueToDeadLock.set(0);
        rollbacksDueToValidation.set(0);
        localReadTime.set(0);
        remoteReadTime.set(0);
        nrLocalReadOp.set(0);
        nrRemoteReadOp.set(0);
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
        return rollbacksDueToValidation.get();
    }

    @ManagedAttribute(description = "Duration of all local read command since last reset (nano-seconds)")
    @Metric(displayName = "LocalReadTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getLocalReadTime() {
        return localReadTime.get();
    }

    @ManagedAttribute(description = "Duration of all remote read command since last reset (nano-seconds)")
    @Metric(displayName = "RemoteReadTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRemoteReadTime() {
        return remoteReadTime.get();
    }

    @ManagedAttribute(description = "Number of local read commands since last reset")
    @Metric(displayName = "NrLocalReadOp", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrLocalReadOp() {
        return nrLocalReadOp.get();
    }

    @ManagedAttribute(description = "Number of remote read commands since last reset")
    @Metric(displayName = "NrRemotelReadOp", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrRemoteReadOp() {
        return nrRemoteReadOp.get();
    }
}
