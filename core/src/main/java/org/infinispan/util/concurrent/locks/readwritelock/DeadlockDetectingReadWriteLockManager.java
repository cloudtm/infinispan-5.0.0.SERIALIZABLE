package org.infinispan.util.concurrent.locks.readwritelock;

import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.xa.DldGlobalTransaction;
import org.infinispan.util.concurrent.locks.DeadlockDetectedException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.concurrent.atomic.AtomicLong;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author pedro
 *         Date: 09-08-2011
 */
@MBean(objectName = "LockManager", description = "Manager that handles MVCC locks (exclusive and shared) " +
        "for entries and it has information about the number of deadlocks that were detected")
public class DeadlockDetectingReadWriteLockManager extends ReadWriteLockManagerImpl {

    private static final Log log = LogFactory.getLog(DeadlockDetectingReadWriteLockManager.class);

    protected volatile long spinDuration;
    protected volatile boolean exposeJmxStats;

    private AtomicLong localTxStopped = new AtomicLong(0);
    private AtomicLong remoteTxStopped = new AtomicLong(0);
    private AtomicLong cannotRunDld = new AtomicLong(0);

    private boolean isDeadlockAndIAmLoosing(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx, Object key) {
        //run the lose check first as it is cheaper
        boolean wouldWeLoose = thisTx.wouldLose(lockOwnerTx);
        if (!wouldWeLoose) {
            if (trace) {
                log.tracef("We (%s) wouldn't lose against the other (%s) transaction, so no point running rest of DLD",
                        thisTx, lockOwnerTx);
            }
            return false;
        }
        //do we have lock on what other tx intends to acquire?
        return ownsLocalIntention(thisTx, lockOwnerTx.getLockIntention()) ||
                ownsRemoteIntention(lockOwnerTx, thisTx, key) || isSameKeyDeadlock(key, thisTx, lockOwnerTx);
    }

    private boolean isSameKeyDeadlock(Object key, DldGlobalTransaction thisTx, DldGlobalTransaction lockOwnerTx) {
        //this relies on the fact that when DLD is enabled a lock is first acquired remotely and then locally
        boolean iHaveRemoteLock = !thisTx.isRemote();
        boolean otherHasLocalLock = lockOwnerTx.isRemote();

        //if we are here then 1) the other tx has a lock on this local key AND 2) I have a lock on the same key remotely
        if (iHaveRemoteLock && otherHasLocalLock) {
            if (trace) {
                log.tracef("Same key deadlock between %s and %s on key %s.", thisTx, lockOwnerTx, key);
            }
            return true;
        }
        return false;
    }

    //This happens with two nodes replicating same tx at the same time.
    private boolean ownsRemoteIntention(DldGlobalTransaction lockOwnerTx, DldGlobalTransaction thisTx, Object key) {
        boolean localLockOwner = !lockOwnerTx.isRemote();
        if (localLockOwner) {
            // I've already acquired lock on this key before replicating here, so this mean we are in deadlock.
            // This assumes the fact that if trying to acquire a remote lock, a tx first acquires a local lock.
            if (thisTx.hasLockAtOrigin(lockOwnerTx)) { //read write lock implementation
                if (trace) {
                    log.tracef("Same key deadlock detected: lock owner tries to acquire lock remotely on %s " +
                            "but we have it!", key);
                }
                return true;
            }
        } else {
            if (trace) {
                log.tracef("Lock owner is remote: %s", lockOwnerTx);
            }
        }
        return false;
    }

    private boolean ownsLocalIntention(DldGlobalTransaction thisTx, Object lockOwnerTxIntention) {
        boolean result = lockOwnerTxIntention != null &&
                lockContainer.ownsReadOrWriteLock(thisTx, lockOwnerTxIntention);
        if (trace) {
            log.tracef("Local intention is '%s'. Do we own lock for it? %s, We == %s",
                    lockOwnerTxIntention, result, thisTx);
        }
        return result;
    }

    private void updateStats(DldGlobalTransaction tx) {
        if (exposeJmxStats) {
            if (tx.isRemote()) {
                remoteTxStopped.incrementAndGet();
            } else {
                localTxStopped.incrementAndGet();
            }
        }
    }

    @Start
    public void init() {
        spinDuration = configuration.getDeadlockDetectionSpinDuration();
        exposeJmxStats = configuration.isExposeJmxStatistics();
    }

    @Override
    public boolean sharedLockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
        long lockTimeout = getLockAcquisitionTimeout(ctx);
        if (trace) {
            log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, lockTimeout);
        }

        if (ctx.isInTxScope()) {

            updateContentionStats(key, ctx);

            if (trace) {
                log.trace("Using early dead lock detection");
            }

            final long start = System.currentTimeMillis();
            DldGlobalTransaction thisTx = (DldGlobalTransaction) ctx.getLockOwner();
            thisTx.setLockIntention(key);

            if (trace) {
                log.tracef("Setting lock intention to: %s", key);
            }

            while (System.currentTimeMillis() < (start + lockTimeout)) {
                if (lockContainer.acquireSharedLock(key, spinDuration, MILLISECONDS) != null) {
                    thisTx.setLockIntention(null); //clear lock intention
                    if (trace) {
                        log.tracef("successfully acquired lock on %s, returning ...", key);
                    }
                    return true;
                } else {
                    Object owner = getOwner(key);
                    if (!(owner instanceof DldGlobalTransaction)) {
                        if (trace) {
                            log.tracef("Not running DLD as lock owner(%s) is not a transaction", owner);
                        }
                        cannotRunDld.incrementAndGet();
                        continue;
                    }
                    DldGlobalTransaction lockOwnerTx = (DldGlobalTransaction) owner;
                    if (isDeadlockAndIAmLoosing(lockOwnerTx, thisTx, key)) {
                        updateStats(thisTx);
                        String message = String.format(
                                "Deadlock found and we %s shall not continue. Other tx is %s",
                                thisTx, lockOwnerTx);
                        if (trace) {
                            log.tracef(message);
                        }
                        throw new DeadlockDetectedException(message);
                    }
                }
            }
        } else {
            if (lockContainer.acquireSharedLock(key, lockTimeout, MILLISECONDS) != null) {
                return true;
            }
        }
        // couldn't acquire lock!
        return false;
    }

    @Override
    public boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
        long lockTimeout = getLockAcquisitionTimeout(ctx);
        if (trace) {
            log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, lockTimeout);
        }

        if (ctx.isInTxScope()) {

            updateContentionStats(key, ctx);

            if (trace) {
                log.trace("Using early dead lock detection");
            }

            final long start = System.currentTimeMillis();
            DldGlobalTransaction thisTx = (DldGlobalTransaction) ctx.getLockOwner();
            thisTx.setLockIntention(key);

            if (trace) {
                log.tracef("Setting lock intention to: %s", key);
            }

            while (System.currentTimeMillis() < (start + lockTimeout)) {
                if (lockContainer.acquireLock(key, spinDuration, MILLISECONDS) != null) {
                    thisTx.setLockIntention(null); //clear lock intention
                    if (trace) {
                        log.tracef("successfully acquired lock on %s, returning ...", key);
                    }
                    return true;
                } else {
                    Object owner = getOwner(key);
                    if (!(owner instanceof DldGlobalTransaction)) {
                        if (trace) {
                            log.tracef("Not running DLD as lock owner(%s) is not a transaction", owner);
                        }
                        cannotRunDld.incrementAndGet();
                        continue;
                    }
                    DldGlobalTransaction lockOwnerTx = (DldGlobalTransaction) owner;
                    if (isDeadlockAndIAmLoosing(lockOwnerTx, thisTx, key)) {
                        updateStats(thisTx);
                        String message = String.format(
                                "Deadlock found and we %s shall not continue. Other tx is %s",
                                thisTx, lockOwnerTx);
                        if (trace) {
                            log.tracef(message);
                        }
                        throw new DeadlockDetectedException(message);
                    }
                }
            }
        } else {
            if (lockContainer.acquireLock(key, lockTimeout, MILLISECONDS) != null) {
                return true;
            }
        }
        // couldn't acquire lock!
        return false;
    }

    public void setExposeJmxStats(boolean exposeJmxStats) {
        this.exposeJmxStats = exposeJmxStats;
    }

    @ManagedAttribute(description = "Total number of local detected deadlocks")
    @Metric(displayName = "Number of total detected deadlocks", measurementType = MeasurementType.TRENDSUP)
    public long getTotalNumberOfDetectedDeadlocks() {
        return localTxStopped.get() + remoteTxStopped.get();
    }

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset statistics")
    public void resetStatistics() {
        localTxStopped.set(0);
        remoteTxStopped.set(0);
        cannotRunDld.set(0);
    }

    @ManagedAttribute(description = "Number of remote transaction that were roll backed due to deadlocks")
    @Metric(displayName = "Number of remote transaction that were roll backed due to deadlocks",
            measurementType = MeasurementType.TRENDSUP)
    public long getDetectedRemoteDeadlocks() {
        return remoteTxStopped.get();
    }

    @ManagedAttribute (description = "Number of local transaction that were roll backed due to deadlocks")
    @Metric(displayName = "Number of local transaction that were roll backed due to deadlocks",
            measurementType = MeasurementType.TRENDSUP)
    public long getDetectedLocalDeadlocks() {
        return localTxStopped.get();
    }

    @ManagedAttribute(description = "Number of situations when we try to determine a deadlock and the other lock " +
            "owner is NOT a transaction. In this scenario we cannot run the deadlock detection mechanism")
    @Metric(displayName = "Number of unsolvable deadlock situations", measurementType = MeasurementType.TRENDSUP)
    public long getOverlapWithNotDeadlockAwareLockOwners() {
        return cannotRunDld.get();
    }

    @ManagedAttribute(description = "Number of locally originated transactions that were interrupted as a deadlock " +
            "situation was detected")
    @Metric(displayName = "Number of interrupted local transactions", measurementType = MeasurementType.TRENDSUP)
    @Deprecated
    public long getLocallyInterruptedTransactions() {
        return -1;
    }
}
