package org.infinispan.totalorder;

import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author pedro
 *         Date: 16-06-2011
 */
@MBean(objectName = "TotalOrderStats", description = "It shows stats about the total order transaction, namely " +
        "transactions aborted, time of TotalOrder self deliver")
public class TotalOrderTransactionManager {
    private ConcurrentMap<GlobalTransaction, LocalTxInformation> localTxs;
    private ConcurrentMap<GlobalTransaction, RemoteTxInformation> remoteTxs;
    private Log log;
    private DataContainer dataContainer;
    private DistributionManager distMan;

    //jmx stats
    private final AtomicLong abortedTx = new AtomicLong(0);
    private final AtomicLong committedTx = new AtomicLong(0);

    private final AtomicLong selfDeliverDuration = new AtomicLong(0);
    private final AtomicLong selfDeliverTxs = new AtomicLong(0);

    private final AtomicLong onePhaseValidationDuration = new AtomicLong(0);
    private final AtomicLong numberOf1PhaseValidations = new AtomicLong(0);

    private final AtomicLong twoPhaseValidationDuration = new AtomicLong(0);
    private final AtomicLong numberOf2PhaseValidations = new AtomicLong(0);

    private final AtomicLong twoPhaseWaitingDuration = new AtomicLong(0);
    private final AtomicLong numberOf2PhaseWaitings = new AtomicLong(0);

    private final AtomicLong localCommitDuration = new AtomicLong(0);
    private final AtomicLong numberOfLocalCommits = new AtomicLong(0);

    private final AtomicLong remoteCommitDuration = new AtomicLong(0);
    private final AtomicLong remoteCommitWaitingDuration = new AtomicLong(0);
    private final AtomicLong numberOfRemoteCommits = new AtomicLong(0);

    private final AtomicLong localRollbackDuration = new AtomicLong(0);
    private final AtomicLong numberOfLocalRollbacks = new AtomicLong(0);

    private final AtomicLong remoteRollbackDuration = new AtomicLong(0);
    private final AtomicLong remoteRollbackWaitingDuration = new AtomicLong(0);
    private final AtomicLong numberOfRemoteRollbacks = new AtomicLong(0);

    private final AtomicLong waitingValidationDuration = new AtomicLong(0);
    private final AtomicLong numberOfWaitingValidations = new AtomicLong(0);


    public TotalOrderTransactionManager(int concurrencyLevel) {
        localTxs = new ConcurrentHashMap<GlobalTransaction, LocalTxInformation>(16,0.75f,concurrencyLevel);
        remoteTxs = new ConcurrentHashMap<GlobalTransaction, RemoteTxInformation>(128,0.75f,concurrencyLevel);
        log = LogFactory.getLog(getClass());
    }

    @Inject
    public void init(DataContainer dataContainer, DistributionManager distMan) {
        this.dataContainer = dataContainer;
        this.distMan = distMan;
    }

    //------------------------- local transactions information and methods ------------------------- //

    public void createLocalTxIfAbsent(GlobalTransaction gtx) {
        localTxs.putIfAbsent(gtx, new LocalTxInformation());
    }

    public void removeLocalTx(GlobalTransaction gtx) {
        localTxs.remove(gtx);
    }

    public LocalTxInformation getLocalTx(GlobalTransaction gtx) {
        return localTxs.get(gtx);
    }

    public Object waitValidation(GlobalTransaction gtx) throws Throwable {
        LocalTxInformation ltx = localTxs.get(gtx);

        if(ltx == null) {
            log.fatalf("trying to wait for a local transaction that wasn't created. transaction is %s",
                    Util.prettyPrintGlobalTransaction(gtx));
            throw new IllegalStateException("trying to wait for a local transaction that wasn't created." +
                    "GlobalTransction is " + Util.prettyPrintGlobalTransaction(gtx));
        }
        ltx.setStartTime();

        if(log.isInfoEnabled()) {
            log.infof("transaction %s is going to wait for validation in total order...",
                    Util.prettyPrintGlobalTransaction(gtx));
        }

        Object retval = ltx.waitValidation();
        if(log.isDebugEnabled()) {
            log.infof("transaction %s have finished to wait for validation in total order... result is %s",
                    Util.prettyPrintGlobalTransaction(gtx),
                    (retval != null ? retval.toString() : "null"));
        }
        if(retval instanceof Throwable) {
            throw (Throwable) retval;
        }
        return retval;
    }

    public void addKeyToBeValidated(GlobalTransaction gtx, Set<Object> keys) {
        LocalTxInformation ltx = localTxs.get(gtx);
        if(ltx != null) {
            ltx.addKeysToValidate(keys);
        }
    }

    public void addVote(GlobalTransaction gtx, boolean success, Set<Object> keys) {
        LocalTxInformation ltx = localTxs.get(gtx);
        if(ltx != null) {
            if(!success) {
                ltx.markAllKeysValidatedAndReturnThisObject(new WriteSkewException("Negative Vote recevied... aborting tx"));
            } else {
                ltx.addKeysSuccessfulyValidated(keys);
            }
        }
    }

    public static class LocalTxInformation {
        private CountDownLatch barrier, allKeysValidated;
        private Object retval;
        private final Set<Object> keysNeedsValidation = new HashSet<Object>();

        //self deliver duration stat
        private long startTime;

        public LocalTxInformation() {
            barrier = new CountDownLatch(1);
            allKeysValidated = new CountDownLatch(1);
            retval = null;
            startTime = -1;
        }

        public Object waitValidation() throws InterruptedException {
            barrier.await();
            allKeysValidated.await();
            return retval;
        }

        public void setRetValue(Object retval) {
            this.retval = retval;
        }

        public void markValidationCompelete() {
            barrier.countDown();
        }

        public void setStartTime() {
            startTime = System.nanoTime();
        }

        public long getStartTime() {
            return startTime;
        }

        public void addKeysToValidate(Set<Object> keys) {
            if(keys.isEmpty()) {
                allKeysValidated.countDown();
            }
            keysNeedsValidation.addAll(keys);
        }

        public synchronized void addKeysSuccessfulyValidated(Set<Object> keys) {
            keysNeedsValidation.removeAll(keys);
            if(keysNeedsValidation.isEmpty()) {
                allKeysValidated.countDown();
            }
        }

        public synchronized void markAllKeysValidatedAndReturnThisObject(Object retval) {
            this.retval = retval;
            allKeysValidated.countDown();
        }
    }

    //------------------------- remote transactions information and methods ------------------------- //

    public RemoteTxInformation createRemoteTxIfAbsent(GlobalTransaction gtx) {
        RemoteTxInformation rtx = new RemoteTxInformation();
        RemoteTxInformation exists = remoteTxs.putIfAbsent(gtx, rtx);
        return exists != null ? exists : rtx;
    }

    public RemoteTxInformation getRemoteTx(GlobalTransaction gtx) {
        return remoteTxs.get(gtx);
    }

    public void removeRemoteTx(GlobalTransaction gtx) {
        remoteTxs.remove(gtx);
    }

    public RemoteTxInformation createRemoteTxAndSetStartTime(GlobalTransaction gtx) {
        RemoteTxInformation remoteTx = createRemoteTxIfAbsent(gtx);
        remoteTx.setStart(System.nanoTime());
        return remoteTx;
    }

    public static class RemoteTxInformation {
        private CountDownLatch validationCompleted, commitOrRollbackCompleted;
        private volatile boolean writeSetReceived, commitOrRollbackReceived;
        private long start;

        public RemoteTxInformation() {
            validationCompleted = new CountDownLatch(1);
            commitOrRollbackCompleted = new CountDownLatch(1);
            start = -1;
        }

        public void validationCompeted() {
            writeSetReceived = true;
            validationCompleted.countDown();
        }

        public boolean canBeRemoved() {
            return writeSetReceived && commitOrRollbackReceived;
        }

        public void waitUntilRollbackPossible() throws InterruptedException {
            validationCompleted.await();
        }

        public void waitUntilCommitPossible() throws InterruptedException {
            validationCompleted.await();
        }

        public void commitOrRollbackCommandReceived() {
            commitOrRollbackReceived = true;
            commitOrRollbackCompleted.countDown();
        }

        public void waitUntilCommitOrRollback() throws InterruptedException {
            commitOrRollbackCompleted.await();
        }

        /*public void markCommitOrRollbackCompleted() {
            commitOrRollbackCompleted.countDown();
        }*/

        public long getStart() {
            return start;
        }

        public void setStart(long start) {
            this.start = start;
        }
    }

    // ---------------------------- WRITE SKEW CHECK METHOD -------------------------------- //
    public boolean checkWriteSkew(Map<Object, Object> originalValues) {
        for(Map.Entry<Object, Object> original : originalValues.entrySet()) {
            Object key = original.getKey();
            Object originglaValue = original.getValue();

            if(distMan != null && !distMan.getLocality(key).isLocal()) {
                continue;
            }

            InternalCacheEntry ice = dataContainer.get(key);
            Object actualValue = ice.getValue();
            if(actualValue != originglaValue && actualValue != null && !actualValue.equals(original.getValue())) {
                if(log.isDebugEnabled()) {
                    log.debugf("write skew check fails for key %s. original value is %s and actual value is %s",
                            original.getKey().toString(),
                            original.getValue().toString(),
                            ice.getValue().toString());
                }
                return false;
            }
        }
        return true;
    }

    // ---------------------------- COLLECT JMX STATS METHODS ----------------------------------------//

    public void incrementTransaction(boolean successful) {
        if(successful) {
            committedTx.incrementAndGet();
        } else {
            abortedTx.incrementAndGet();
        }
    }

    public void setSelfDeliverTxDuration(GlobalTransaction gtx) {
        LocalTxInformation ltx = localTxs.get(gtx);
        if(ltx != null) {
            long start = ltx.getStartTime();
            long end = System.nanoTime();
            if(start != -1) {
                selfDeliverDuration.addAndGet(end - start);
                selfDeliverTxs.incrementAndGet();
            }

        }
    }

    public void addValidationDuration(long duration, boolean isOnePhase) {
        if(duration > 0) {
            if(isOnePhase) {
                onePhaseValidationDuration.addAndGet(duration);
                numberOf1PhaseValidations.incrementAndGet();
            } else {
                twoPhaseValidationDuration.addAndGet(duration);
                numberOf2PhaseValidations.incrementAndGet();
            }
        }
    }

    public void add2PhaseWaitingDuration(long duration) {
        if(duration > 0) {
            twoPhaseWaitingDuration.addAndGet(duration);
            numberOf2PhaseWaitings.incrementAndGet();
        }
    }

    public void addRemoteDuration(long commandDuration, long commandWaitingDuration, boolean commit) {
        AtomicLong dur;
        AtomicLong waitDur;
        AtomicLong nr;
        if(commit) {
            dur = remoteCommitDuration;
            waitDur = remoteCommitWaitingDuration;
            nr = numberOfRemoteCommits;
        } else {
            dur = remoteRollbackDuration;
            waitDur = remoteRollbackWaitingDuration;
            nr = numberOfRemoteRollbacks;
        }
        if(commandDuration > 0 && commandWaitingDuration > 0) {
            dur.addAndGet(commandDuration);
            waitDur.addAndGet(commandWaitingDuration);
            nr.incrementAndGet();
        }
    }

    public void addLocalDuration(long commandDuration, boolean commit) {
        AtomicLong dur;
        AtomicLong nr;
        if(commit) {
            dur = localCommitDuration;
            nr = numberOfLocalCommits;
        } else {
            dur = localRollbackDuration;
            nr = numberOfLocalRollbacks;
        }
        if(commandDuration > 0) {
            dur.addAndGet(commandDuration);
            nr.incrementAndGet();
        }
    }

    public void addWaitingValidation(long duration) {
        if(duration > 0) {
            waitingValidationDuration.addAndGet(duration);
            numberOfWaitingValidations.incrementAndGet();
        }
    }

    // -------------------------------- JMX STATS ------------------------------------//

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics")
    public void resetStatistics() {
        committedTx.set(0);
        abortedTx.set(0);

        selfDeliverDuration.set(0);
        selfDeliverTxs.set(0);
        onePhaseValidationDuration.set(0);
        numberOf1PhaseValidations.set(0);

        twoPhaseValidationDuration.set(0);
        numberOf2PhaseValidations.set(0);

        twoPhaseWaitingDuration.set(0);
        numberOf2PhaseWaitings.set(0);

        localCommitDuration.set(0);
        numberOfLocalCommits.set(0);

        remoteCommitDuration.set(0);
        remoteCommitWaitingDuration.set(0);
        numberOfRemoteCommits.set(0);

        localRollbackDuration.set(0);
        numberOfLocalRollbacks.set(0);

        remoteRollbackDuration.set(0);
        remoteRollbackWaitingDuration.set(0);
        numberOfRemoteRollbacks.set(0);

        waitingValidationDuration.set(0);
        numberOfWaitingValidations.set(0);
    }

    @ManagedAttribute(description = "returns the number of committed transactions on this component (local + remote)")
    @Metric(displayName = "CommittedTx", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getCommittedTx() {
        return committedTx.get();
    }

    @ManagedAttribute(description = "returns the number of aborted transactions on this component, except that ones that invoked rollback() method")
    @Metric(displayName = "AbortedTx", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAbortedTx() {
        return abortedTx.get();
    }

    @ManagedAttribute(description = "returns the average time that one transaction takes between the send and receive in Total Order")
    @Metric(displayName = "AvgSelfDeliverDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgSelfDeliverDuration() {
        long selfDeliv = selfDeliverTxs.get();
        if(selfDeliv != 0) {
            return selfDeliverDuration.get() / selfDeliv;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one transaction takes to be validated in Total Order in One Phase Commit")
    @Metric(displayName = "Avg1PhaseValidationDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvg1PhaseValidationDuration() {
        long nr = numberOf1PhaseValidations.get();
        if(nr != 0) {
            return onePhaseValidationDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one transaction takes to be validated in Total Order in One Phase Commit")
    @Metric(displayName = "Avg2PhaseValidationDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvg2PhaseValidationDuration() {
        long nr = numberOf2PhaseValidations.get();
        if(nr != 0) {
            return twoPhaseValidationDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one transaction takes to wait for the second phase, ie, the commit/rollback command")
    @Metric(displayName = "Avg2PhaseWaitingDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvg2PhaseWaitingDuration() {
        long nr = numberOf2PhaseWaitings.get();
        if(nr != 0) {
            return twoPhaseWaitingDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one local commit command takes to be processed")
    @Metric(displayName = "AvgLocalCommitDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgLocalCommitDuration() {
        long nr = numberOfLocalCommits.get();
        if(nr != 0) {
            return localCommitDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one remote commit command takes to be processed")
    @Metric(displayName = "AvgRemoteCommitDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgRemoteCommitDuration() {
        long nr = numberOfRemoteCommits.get();
        if(nr != 0) {
            return remoteCommitDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one remote commit command waits until can be processed")
    @Metric(displayName = "AvgRemoteCommitWaitingDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgRemoteCommitWaitingDuration() {
        long nr = numberOfRemoteCommits.get();
        if(nr != 0) {
            return remoteCommitWaitingDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one local rollback command takes (except when rollback() method is invoked) to be processed")
    @Metric(displayName = "AvgLocalRollbackDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgLocalRollbackDuration() {
        long nr = numberOfLocalRollbacks.get();
        if(nr != 0) {
            return localRollbackDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one remote rollback command takes to be processed")
    @Metric(displayName = "AvgRemoteRollbackDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgRemoteRollbackDuration() {
        long nr = numberOfRemoteRollbacks.get();
        if(nr != 0) {
            return remoteRollbackDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that one remote rollback command waits until can be processed")
    @Metric(displayName = "AvgRemoteRollbackWaitingDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgRemoteRollbackWaitingDuration() {
        long nr = numberOfRemoteRollbacks.get();
        if(nr != 0) {
            return remoteRollbackWaitingDuration.get() / nr;
        } else {
            return 0;
        }
    }

    @ManagedAttribute(description = "returns the average time that a transactions waits between it was received and starts to be validated")
    @Metric(displayName = "AvgWaitingValidationDuration", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getAvgWaitingValidationDuration() {
        long nr = numberOfWaitingValidations.get();
        if(nr != 0) {
            return waitingValidationDuration.get() / nr;
        } else {
            return 0;
        }
    }
}
