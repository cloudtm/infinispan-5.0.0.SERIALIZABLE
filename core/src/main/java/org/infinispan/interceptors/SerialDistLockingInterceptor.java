package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.container.MultiVersionDataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.mvcc.CommitLog;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.readwritelock.ReadWriteLockManager;

import java.util.*;

/**
 * @author pruivo
 *         Date: 23/09/11
 */
public class SerialDistLockingInterceptor extends DistLockingInterceptor implements CommitQueue.CommitInstance {
    private CommitQueue commitQueue;
    private DistributionManager distributionManager;

    private boolean debug, info;
    private CommitLog commitLog;

    @Inject
    public void inject(CommitQueue commitQueue, DistributionManager distributionManager, CommitLog commitLog) {
        this.commitQueue = commitQueue;
        this.distributionManager = distributionManager;
        this.commitLog = commitLog;
    }

    @Start
    public void updateDebugBoolean() {
        debug = log.isDebugEnabled();
        info = log.isInfoEnabled();
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        if(ctx.isInTxScope()) {
            int pos = getPositionInVC(command.getKey());
            TxInvocationContext txctx = (TxInvocationContext) ctx;
            long min = txctx.getVectorClockValueIn(pos);
            if(isKeyLocal(command.getKey()) && !commitLog.waitUntilMinVersionIsGuaranteed(min, pos,
                    configuration.getSyncReplTimeout() * 3)) {
                log.warnf("Try to read the key [%s] (local) but the min version is not guaranteed!",
                        command.getKey());
                throw new TimeoutException("Cannot read the key " + command.getKey());
            }
        }

        return super.visitGetKeyValueCommand(ctx, command);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        if(trace) {
            log.tracef("Commit Command received for transaction %s (%s)",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    (ctx.isOriginLocal() ? "local" : "remote"));
        }
        try {
            return invokeNextInterceptor(ctx, command);
        } finally {
            if (ctx.isInTxScope()) {
                if(getOnlyLocalKeys(ctx.getAffectedKeys()).isEmpty()) {
                    ((ReadWriteLockManager)lockManager).unlockAfterCommit(ctx);
                } else {
                    try {
                        commitQueue.updateAndWait(command.getGlobalTransaction(), command.getCommitVersion());

                        ((ReadWriteLockManager)lockManager).unlockAfterCommit(ctx);
                    } catch(Exception e) {
                        if(debug) {
                            log.debugf("An exception was caught in commit command (tx:%s, ex: %s: %s)",
                                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                                    e.getClass().getName(), e.getMessage());
                        }
                        e.printStackTrace();
                        commitQueue.remove(command.getGlobalTransaction());
                    }
                }
            } else {
                throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional " +
                        "context in scope. " + ctx);
            }
        }
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        if(trace) {
            log.tracef("Rollback Command received for transaction %s (%s)",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    (ctx.isOriginLocal() ? "local" : "remote"));
        }
        try {
            commitQueue.remove(command.getGlobalTransaction());
            return invokeNextInterceptor(ctx, command);
        } finally {
            if (ctx.isInTxScope()) {
                cleanupLocks(ctx, false, null);
            } else {
                throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional " +
                        "context in scope. " + ctx);
            }
        }
    }

    @Override
    public Object visitAcquireValidationLocksCommand(TxInvocationContext ctx, AcquireValidationLocksCommand command)
            throws Throwable {
        if(trace) {
            log.tracef("Acquire validation locks received for transaction %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        }
        ReadWriteLockManager RWLMan = (ReadWriteLockManager) lockManager;
        Object actualKeyInValidation = null;
        try {
            //first acquire the write locks. if it needs the read lock for a written key, then it will be no problem
            //acquire it

            if(debug) {
                log.debugf("Acquire locks on transaction's [%s] write set %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), command.getWriteSet());
            }

            for(Object k : command.getWriteSet()) {
                if(!RWLMan.lockAndRecord(k, ctx)) {
                    Object owner = lockManager.getOwner(k);
                    // if lock cannot be acquired, expose the key itself, not the marshalled value
                    if (k instanceof MarshalledValue) {
                        k = ((MarshalledValue) k).get();
                    }
                    throw new TimeoutException("Unable to acquire lock on key [" + k + "] for requestor [" +
                            ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
                }
                if(!ctx.getLookedUpEntries().containsKey(k)) {
                    //putLookedUpEntry can throw an exception. if k is not saved, then it will be locked forever
                    actualKeyInValidation = k;

                    // put null initially. when the write commands will be replayed, they will overwrite it.
                    // it is necessary to release later (if validation fails)
                    ctx.putLookedUpEntry(k, null);
                }
            }

            if(debug) {
                log.debugf("Acquire locks (and validate) on transaction's [%s] read set %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), command.getReadSet());
            }

            for(Object k : command.getReadSet()) {
                if(!RWLMan.sharedLockAndRecord(k, ctx)) {
                    Object owner = lockManager.getOwner(k);
                    // if lock cannot be acquired, expose the key itself, not the marshalled value
                    if (k instanceof MarshalledValue) {
                        k = ((MarshalledValue) k).get();
                    }
                    throw new TimeoutException("Unable to acquire lock on key [" + k + "] for requestor [" +
                            ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
                }
                if(!ctx.getLookedUpEntries().containsKey(k)) {
                    //see comments above
                    actualKeyInValidation = k;
                    ctx.putLookedUpEntry(k, null);
                }
                validateKey(k, command.getVersion());
            }

            actualKeyInValidation = null;
            //it does no need to passes down in the chain
            //but it is done to keep compatibility (or if we want to add a new interceptor later)
            Object retVal =  invokeNextInterceptor(ctx, command);

            if(info) {
                log.infof("Validation of transaction [%s] succeeds",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            }

            return retVal;
        } catch(Throwable t) {
            //if some exception occurs in method putLookedUpEntry this key must be unlocked too
            if(actualKeyInValidation != null) {
                RWLMan.unlock(actualKeyInValidation);
            }

            RWLMan.unlock(ctx);

            if(info) {
                log.infof("Validation of transaction [%s] fails. Reason: %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), t.getMessage());
            }
            throw t;
        }
    }

    protected void validateKey(Object key, VersionVC toCompare) {
        //it only needs to compare it own group id because it is guaranteed that this position is increase in
        // each version!
        int pos = getPositionInVC(key);
        if(!dataContainer.validateKey(key, pos, toCompare.get(pos))) {
            throw new CacheException("Validation of key [" + key + "] failed!");
        }
    }

    private void commitEntry(CacheEntry entry, VersionVC version) {
        if(trace) {
            log.tracef("Commit Entry %s with version %s", entry, version);
        }
        boolean doCommit = true;
        if (!dm.getLocality(entry.getKey()).isLocal()) {
            if (configuration.isL1CacheEnabled()) {
                dm.transformForL1(entry);
            } else {
                doCommit = false;
            }
        }
        if (doCommit) {
            if(entry instanceof SerializableEntry) {
                ((SerializableEntry) entry).commit(dataContainer, version);
            } else {
                entry.commit(dataContainer);
            }
        } else {
            entry.rollback();
        }
    }

    private void cleanupLocks(InvocationContext ctx, boolean commit, VersionVC commitVersion) {
        if (commit) {
            ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
            Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
            if (trace) {
                log.tracef("Commit modifications. Number of entries in context: %s, commit version: %s",
                        entries.size(), commitVersion);
            }
            while (it.hasNext()) {
                Map.Entry<Object, CacheEntry> e = it.next();
                CacheEntry entry = e.getValue();
                Object key = e.getKey();
                // could be null (if it was read and not written)
                if (entry != null && entry.isChanged()) {
                    commitEntry(entry, commitVersion);
                } else {
                    if (trace) {
                        log.tracef("Entry for key %s is null, not calling commitUpdate", key);
                    }
                }
            }

            //commitVersion is null when the transaction is read-only
            if(ctx.isInTxScope() && commitVersion != null) {
                ((MultiVersionDataContainer) dataContainer).addNewCommittedTransaction(commitVersion);
            }

            //this not call the entry.rollback() (instead of releaseLocks(ctx))
            ((ReadWriteLockManager)lockManager).unlockAfterCommit(ctx);

        } else {
            lockManager.releaseLocks(ctx);
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

    @Override
    public void commit(InvocationContext ctx, VersionVC commitVersion) {
        ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
        Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
        if (trace) {
            log.tracef("Commit modifications. Number of entries in context: %s, commit version: %s",
                    entries.size(), commitVersion);
        }
        while (it.hasNext()) {
            Map.Entry<Object, CacheEntry> e = it.next();
            CacheEntry entry = e.getValue();
            Object key = e.getKey();
            // could be null (if it was read and not written)
            if (entry != null && entry.isChanged()) {
                commitEntry(entry, commitVersion);
            } else {
                if (trace) {
                    log.tracef("Entry for key %s is null, not calling commitUpdate", key);
                }
            }
        }
    }

    @Override
    public void addTransaction(VersionVC commitVC) {
        ((MultiVersionDataContainer) dataContainer).addNewCommittedTransaction(commitVC);
    }

    @Override
    public void addTransaction(List<VersionVC> commitVC) {
        ((MultiVersionDataContainer) dataContainer).addNewCommittedTransaction(commitVC);
    }

    private int getPositionInVC(Object key) {
        return dm.locateGroup(key).getId();
    }

    private boolean isKeyLocal(Object key) {
        return distributionManager.getLocality(key).isLocal();
    }
}
