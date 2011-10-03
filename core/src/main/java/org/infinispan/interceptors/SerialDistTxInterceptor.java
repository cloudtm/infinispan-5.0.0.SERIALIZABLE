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
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.mvcc.CommitLog;
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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author pruivo
 *         Date: 22/09/11
 * WARNING: this only works for put() and get(). others methods like putAll(), entrySet(), keySet()
 * and so on, it is not implemented
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class SerialDistTxInterceptor extends DistTxInterceptor {
    private CommandsFactory commandsFactory;
    private CommitQueue commitQueue;
    private InvocationContextContainer icc;
    private DistributionManager distributionManager;
    private CommitLog commitLog;

    private boolean info, debug;

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
    public void inject(CommandsFactory commandsFactory, CommitQueue commitQueue, InvocationContextContainer icc,
                       DistributionManager distributionManager, CommitLog commitLog) {
        this.commandsFactory = commandsFactory;
        this.commitQueue = commitQueue;
        this.icc = icc;
        this.distributionManager = distributionManager;
        this.commitLog = commitLog;
    }

    @Start
    public void setDebugBoolean() {
        info = log.isInfoEnabled();
        debug = log.isDebugEnabled();
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        long start = System.nanoTime();
        boolean successful = true;
        try {
            if(trace) {
                log.tracef("Prepare Command received for transaction %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            }

            Set<Object> writeSet = getOnlyLocalKeys(command.getAffectedKeys());
            Set<Object> readSet = getOnlyLocalKeys(command.getReadSet());

            if(ctx.isOriginLocal() && command.getAffectedKeys().isEmpty()) {
                if(debug) {
                    log.debugf("Transaction [%s] is read-only and it wants to prepare. ignore it and return.",
                            Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
                }
                //read-only transaction has a valid snapshot
                return null;
            }

            if(debug) {
                log.debugf("Transaction [%s] wants to prepare. read set: %s, write set: %s, version :%s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                        readSet, writeSet, command.getVersion());
            }

            //it acquires the read-write locks and it validates the read set
            AcquireValidationLocksCommand locksCommand = commandsFactory.buildAcquireValidationLocksCommand(
                    command.getGlobalTransaction(), readSet, writeSet, command.getVersion());
            invokeNextInterceptor(ctx, locksCommand);

            if(debug) {
                log.debugf("Transaction [%s] passes validation",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            }

            //(this is equals to old schemes) wrap the wrote entries
            //finally, process the rest of the command
            Object retVal = super.visitPrepareCommand(ctx, command);

            VersionVC commitVC;

            if(!writeSet.isEmpty()) {
                commitVC = commitQueue.addTransaction(command.getGlobalTransaction(), ctx.calculateVersionToRead(),
                        icc.getInvocationContext().clone(), getVCPositions(writeSet));

                if(info) {
                    log.infof("Transaction %s can commit. It was added to commit queue. " +
                            "The 'temporary' commit version is %s",
                            Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                            commitVC);
                }
            } else {
                //if it is readonly, return the most recent version
                commitVC = commitLog.getActualVersion();

                if(info) {
                    log.infof("Transaction %s can commit. It is read-only on this node. " +
                            "The 'temporary' commit version is %s",
                            Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                            commitVC);
                }
            }


            if(retVal != null && retVal instanceof VersionVC) {
                //the retVal has the maximum vector clock of all involved nodes
                //this in only performed in the node that executed the transaction (I hope)
                VersionVC othersCommitVC = (VersionVC) retVal;
                commitVC.setToMaximum(othersCommitVC);
                calculateCommitVC(commitVC, getVCPositions(writeSet));
            }

            ctx.setCommitVersion(commitVC);

            if(debug) {
                log.debugf("Transaction [%s] %s commit version is %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                        (ctx.isOriginLocal() ? "final" : "temporary"),
                        commitVC);
            }

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
        if(info) {
            log.infof("Commit Command received for transaction %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        }

        return super.visitCommitCommand(ctx, command);
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        if(info) {
            log.infof("Rollback Command received for transaction %s", Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        }
        return super.visitRollbackCommand(ctx, command);
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        long start = System.nanoTime();
        try {
            Object retVal = super.visitGetKeyValueCommand(ctx, command);

            if(ctx.isInTxScope()) {
                TxInvocationContext txctx = (TxInvocationContext) ctx;
                String gtxID = Util.prettyPrintGlobalTransaction(txctx.getGlobalTransaction());

                int pos = getPositionInVC(command.getKey());
                txctx.markReadFrom(pos);

                //update vc
                InternalMVCCEntry ime = ctx.getReadKey(command.getKey());

                if(ime == null) {
                    log.warnf("InternalMVCCEntry is null for key %s in transaction %s",
                            command.getKey(), gtxID);
                } else if(txctx.hasModifications() && !ime.isMostRecent()) {
                    //read an old value... the tx will abort in commit,
                    //so, do not waste time and abort it now
                    if(info) {
                        log.infof("Read-Write transaction [%s] read an old value. It will be aborted",
                                gtxID);
                    }
                    throw new CacheException("Read-Write Transaction read an old value");
                } else {
                    VersionVC v = ime.getVersion();

                    if(v.get(pos) == VersionVC.EMPTY_POSITION) {
                        v.set(pos,0);
                    }

                    if(debug) {
                        log.debugf("Transaction [%s] read key [%s] and visible vector clock is %s (Group_ID: %s)." +
                                "return value is %s",
                                gtxID, command.getKey(), v, pos,
                                ime.getValue() != null ? ime.getValue().getValue() : "null");
                    }
                    txctx.updateVectorClock(v);
                }
            }

            //remote get
            if(!ctx.isOriginLocal()) {
                if(debug) {
                    log.debugf("Remote Get received for key %s. return value is %s. Multi Version return is %s",
                            command.getKey(), retVal, ctx.getReadKey(command.getKey()));
                }

                if(ctx.readBasedOnVersion()) {
                    retVal = ctx.getReadKey(command.getKey());
                }
            }

            return retVal;
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

    /**
     *
     * @param keys keys to check
     * @return return only the local key or empty if it has no one
     */
    private Set<Object> getOnlyLocalKeys(Set<Object> keys) {
        Set<Object> localKeys = new HashSet<Object>();
        for(Object key : keys) {
            if(distributionManager.getLocality(key).isLocal()) {
                localKeys.add(key);
            }
        }
        return localKeys;
    }

    private Integer[] getVCPositions(Set<Object> writeSet) {
        Set<Integer> positions = new HashSet<Integer>();
        for(Object key : writeSet) {
            positions.add(getPositionInVC(key));
        }
        Integer[] retVal = new Integer[positions.size()];
        positions.toArray(retVal);

        if(debug) {
            log.debugf("Groups IDs for %s are %s", writeSet, retVal);
        }

        return retVal;
    }

    private int getPositionInVC(Object key) {
        return distributionManager.locateGroup(key).getId();
    }

    private void calculateCommitVC(VersionVC vc, Integer[] writeGroups) {
        //first, calculate the maximum value in the write groups
        String originalVC = null;
        if(debug) {
            originalVC = vc.toString();
        }

        long maxValue = 0;
        for(Integer pos : writeGroups) {
            long val = vc.get(pos);
            if(val > maxValue) {
                maxValue = val;
            }
        }

        //second, set the write groups position to the maximum
        for(Integer pos : writeGroups) {
            vc.set(pos, maxValue);
        }

        if(debug) {
            log.debugf("Calculate the commit version from %s. The modified groups are %s and the final" +
                    " version is %s", originalVC, writeGroups, vc);
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
