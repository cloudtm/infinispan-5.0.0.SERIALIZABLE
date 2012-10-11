package org.infinispan.util.concurrent.locks.readwritelock;

import org.infinispan.config.Configuration;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.concurrent.locks.containers.readwritelock.*;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.DataType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;

import javax.transaction.Transaction;
import javax.transaction.TransactionManager;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * @author pedro
 *         Date: 09-08-2011
 */
@MBean(objectName = "LockManager", description = "Manager that handles MVCC locks (exclusive and shared) for entries")
public class ReadWriteLockManagerImpl implements ReadWriteLockManager {

    private static final String ANOTHER_THREAD = "(another thread)";

    protected Configuration configuration;
    protected ReadWriteLockContainer lockContainer;
    private TransactionManager transactionManager;
    private InvocationContextContainer invocationContextContainer;

    private static final Log log = LogFactory.getLog(ReadWriteLockManagerImpl.class);
    protected static final boolean trace = log.isTraceEnabled();

    //stats
    private AtomicLong localLocalContentions = new AtomicLong(0);
    private AtomicLong localRemoteContentions = new AtomicLong(0);
    private AtomicLong remoteLocalContentions = new AtomicLong(0);
    private AtomicLong remoteRemoteContentions = new AtomicLong(0);


    private AtomicLong registerReadTime = new AtomicLong(0);
    private AtomicLong numRegisterRead  = new AtomicLong(0);

    protected void updateContentionStats(Object key, InvocationContext ctx){
        Object owner = getOwner(key);

        if(owner == null) {
            return;
        }

        if(!(owner instanceof GlobalTransaction)) {
            log.warnf("update contention stats invoked with transactional context but owner is not a transaction. context=%s, key=%s, owner=%s",
                    ctx, key, owner);
            return;
        }

        GlobalTransaction holder = (GlobalTransaction)owner;

        GlobalTransaction me = (GlobalTransaction)ctx.getLockOwner();
        if(holder != me){
            boolean amILocal= !(me.isRemote());
            boolean isItLocal= !(holder.isRemote());
            if(amILocal && isItLocal) {
                this.localLocalContentions.incrementAndGet();
            } else if(amILocal && !isItLocal) {
                this.localRemoteContentions.incrementAndGet();
            } else if(isItLocal) {
                this.remoteLocalContentions.incrementAndGet();
            } else {
                this.remoteRemoteContentions.incrementAndGet();
            }
        }
    }

    @Stop
    public void stop() {
        lockContainer.clear();

    }

    @Start
    public void start() {
        localLocalContentions.set(0);
        localRemoteContentions.set(0);
        remoteLocalContentions.set(0);
        remoteRemoteContentions.set(0);
    }

    public long getLockAcquisitionTimeout(InvocationContext ctx) {
        return ctx.hasFlag(Flag.ZERO_LOCK_ACQUISITION_TIMEOUT) ?
                0 : configuration.getLockAcquisitionTimeout();
    }

    @Inject
    public void injectDependencies(Configuration configuration, TransactionManager transactionManager, InvocationContextContainer invocationContextContainer) {
        this.configuration = configuration;
        this.transactionManager = transactionManager;
        this.invocationContextContainer = invocationContextContainer;
    }

    @Start
    public void startLockManager() {
        lockContainer = configuration.isUseLockStriping() ?
                transactionManager == null ? new ReentrantStripedReadWriteLockContainer(configuration.getConcurrencyLevel()) : new OwnableReentrantStripedReadWriteLockContainer(configuration.getConcurrencyLevel(), invocationContextContainer) :
                transactionManager == null ? new ReentrantPerEntryReadWriteLockContainer(configuration.getConcurrencyLevel()) : new OwnableReentrantPerEntryReadWriteLockContainer(configuration.getConcurrencyLevel(), invocationContextContainer);
    }

    @Override
    public boolean sharedLockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
        if(ctx.isInTxScope()) {
            updateContentionStats(key, ctx);
        }

        long lockTimeout = getLockAcquisitionTimeout(ctx);
        if (trace) log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, lockTimeout);
        if (lockContainer.acquireSharedLock(key, lockTimeout, MILLISECONDS) != null) {
            if (ctx instanceof TxInvocationContext) {
                TxInvocationContext tctx = (TxInvocationContext) ctx;
                if (!tctx.isTransactionValid()) {
                    Transaction tx = tctx.getTransaction();
                    log.debugf("Successfully acquired lock, but the transaction %s is no longer valid!  Releasing lock.", tx);
                    lockContainer.releaseSharedLock(key);
                    throw new IllegalStateException("Transaction " + tx + " appears to no longer be valid!");
                }
            }
            if (trace) log.trace("Successfully acquired lock!");
            return true;
        }
        return false;
    }

    @Override
    public void sharedUnlock(Object key) {
        lockContainer.releaseSharedLock(key);
    }

    @Override
    public boolean lockAndRecord(Object key, InvocationContext ctx) throws InterruptedException {
        // the implementation is the same as LockManagerImpl
        if(ctx.isInTxScope()) {
            updateContentionStats(key, ctx);
        }

        long lockTimeout = getLockAcquisitionTimeout(ctx);
        if (trace) log.tracef("Attempting to lock %s with acquisition timeout of %s millis", key, lockTimeout);
        if (lockContainer.acquireLock(key, lockTimeout, MILLISECONDS) != null) {
            if (ctx instanceof TxInvocationContext) {
                TxInvocationContext tctx = (TxInvocationContext) ctx;
                if (!tctx.isTransactionValid()) {
                    Transaction tx = tctx.getTransaction();
                    log.debugf("Successfully acquired lock, but the transaction %s is no longer valid!  Releasing lock.", tx);
                    lockContainer.releaseLock(key);
                    throw new IllegalStateException("Transaction "+tx+" appears to no longer be valid!");
                }
            }
            if (trace) log.trace("Successfully acquired lock!");
            return true;
        }
        return false;
    }

    @Override
    public void unlock(Object key) {
        log.warnf("write lock %s released", key);
        lockContainer.releaseLock(key);
    }

    @Override
    public void unlock(InvocationContext ctx) {
        ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
        if (!entries.isEmpty()) {
            // unlocking needs to be done in reverse order.
            Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
            while (it.hasNext()) {
                Map.Entry<Object, CacheEntry> e = it.next();
                CacheEntry entry = e.getValue();

                // has been locked!
                Object k = e.getKey();
                if (trace) log.tracef("Attempting to unlock %s", k);

                try {
                    lockContainer.releaseSharedLock(k);
                } catch (IllegalMonitorStateException imse) {
                    //no-op
                }
                try {
                    lockContainer.releaseLock(k);
                } catch (IllegalMonitorStateException imse) {
                    //no-op
                }
            }
        }
    }

    @Override
    public boolean ownsLock(Object key, Object owner) {
        return lockContainer.ownsLock(key, owner);
    }

    @Override
    public boolean isLocked(Object key) {
        return lockContainer.isLocked(key);
    }

    @Override
    public Object getOwner(Object key) {
        if (lockContainer.isLocked(key)) {
            ReadWriteLock l = lockContainer.getReadWriteLock(key);

            if (l instanceof OwnableReentrantReadWriteLock) {
                return ((OwnableReentrantReadWriteLock) l).getOwner();
            } else {
                // cannot determine owner, JDK Reentrant locks only provide best-effort guesses.
                return ANOTHER_THREAD;
            }
        } else {
            return null;
        }
    }

    @Override
    public String printLockInfo() {
        return lockContainer.toString();
    }

    @Override
    public boolean possiblyLocked(CacheEntry entry) {
        return entry == null || entry.isChanged() || entry.isNull() || entry.isLockPlaceholder();
    }

    @Override
    public void releaseLocks(InvocationContext ctx) {
        Object owner = ctx.getLockOwner();

        // clean up.
        // unlocking needs to be done in reverse order.
        ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
        Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();

        if (trace) log.tracef("Number of entries in context: %s", entries.size());

        while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            Object key = e.getKey();
            // could be null with read-committed
            if (entry != null && entry.isChanged()) {
                entry.rollback();
            } else {
                if (trace) log.tracef("Entry for key %s is null, not calling rollback()", key);
            }
            // and then unlock

            if (trace) log.tracef("Releasing lock on [%s] for owner %s", key, owner);

            //first release the read lock and then the write lock... the lock acquisition is done
            //in the reverse order, ie, first the write and then the read.
            try {
                lockContainer.releaseSharedLock(key);
            } catch (IllegalMonitorStateException imse) {
                //no-op
            }
            try {
                lockContainer.releaseLock(key);
            } catch (IllegalMonitorStateException imse) {
                //no-op
            }

        }
    }

    @Override
    public int getLockId(Object key) {
        return lockContainer.getLockId(key);
    }

    @Override
    public void unlockAfterCommit(InvocationContext ctx) {
        ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
        if (!entries.isEmpty()) {
            // unlocking needs to be done in reverse order.
            Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
            while (it.hasNext()) {
                Object k = it.next().getKey();
                if (trace) {
                    log.tracef("Attempting to unlock %s", k);
                }

                //first release the read lock and then the write lock... the lock acquisition is done
                //in the reverse order, ie, first the write and then the read.
                try {
                    lockContainer.releaseSharedLock(k);
                } catch (IllegalMonitorStateException imse) {
                    //no-op
                }
                try {
                    lockContainer.releaseLock(k);
                } catch (IllegalMonitorStateException imse) {
                    //no-op
                }
            }
        }
    }


    public void addRegisterReadSample(long nanotime){

        registerReadTime.addAndGet(nanotime);
        numRegisterRead.incrementAndGet();
    }

    /*
    * ======================= JMX == STATS =============================
    */

    @ManagedAttribute(description = "The concurrency level that the MVCC Lock Manager has been configured with.")
    @Metric(displayName = "Concurrency level", dataType = DataType.TRAIT)
    public int getConcurrencyLevel() {
        return configuration.getConcurrencyLevel();
    }

    @ManagedAttribute(description = "The number of exclusive locks that are held.")
    @Metric(displayName = "Number of locks held")
    public int getNumberOfLocksHeld() {
        return lockContainer.getNumLocksHeld();
    }

    @ManagedAttribute(description = "The number of exclusive locks that are available.")
    @Metric(displayName = "Number of locks available")
    public int getNumberOfLocksAvailable() {
        return lockContainer.size() - lockContainer.getNumLocksHeld();
    }

    @ManagedAttribute(description = "The number of contentions among local transactions")
    @Metric(displayName = "LocalLocalContentions")
    public long getLocalLocalContentions(){
        return localLocalContentions.get();
    }

    @ManagedAttribute(description = "The number of contentions among local transactions with remote ones")
    @Metric(displayName = "LocalRemoteContentions")
    public long getLocalRemoteContentions(){
        return localRemoteContentions.get();
    }

    @ManagedAttribute(description = "The number of contentions among remote transactions and local ones")
    @Metric(displayName = "RemoteLocalContentions")
    public long getRemoteLocalContentions(){
        return remoteLocalContentions.get();
    }

    @ManagedAttribute(description = "The number of contentions among remote transactions")
    @Metric(displayName = "RemoteRemoteContentions")
    public long getRemoteRemoteContentions(){
        return remoteRemoteContentions.get();
    }


    @ManagedAttribute(description = "The number of registerRead calls")
    @Metric(displayName = "NumRegisterRead")
    public long getNumRegisterRead(){
        return numRegisterRead.get();
    }

    @ManagedAttribute(description = "The time for registring a read")
    @Metric(displayName = "RegisterReadTime")
    public long getRegisterReadTime(){
        return registerReadTime.get();
    }
}
