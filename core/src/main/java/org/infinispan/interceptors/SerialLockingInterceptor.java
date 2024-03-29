package org.infinispan.interceptors;

import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.container.MultiVersionDataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.mvcc.exception.ValidationException;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.readwritelock.ReadWriteLockManager;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * @author pedro
 *         Date: 25-08-2011
 */
public class SerialLockingInterceptor extends LockingInterceptor implements CommitQueue.CommitInstance {

    private CommitQueue commitQueue;
    private VersionVCFactory versionVCFactory;

    @Inject
    public void inject(CommitQueue commitQueue, VersionVCFactory versionVCFactory) {
        this.commitQueue = commitQueue;
        this.versionVCFactory=versionVCFactory;
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        try {
            return invokeNextInterceptor(ctx, command);
        } finally {
            if (ctx.isInTxScope()) {
                try {
                    commitQueue.updateAndWait(command.getGlobalTransaction(), command.getCommitVersion());

                    ((ReadWriteLockManager)lockManager).unlockAfterCommit(ctx);
                } catch(Exception e) {
                    e.printStackTrace();
                    commitQueue.remove(command.getGlobalTransaction(), true);
                }
            } else {
                throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional context in scope. " + ctx);
            }
        }
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        commitQueue.remove(command.getGlobalTransaction(), false);
        try {
            return invokeNextInterceptor(ctx, command);
        } finally {
            if (ctx.isInTxScope()) {
                cleanupLocks(ctx, false, null);
            } else {
                throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional context in scope. " + ctx);
            }
        }
    }

    @Override
    public Object visitAcquireValidationLocksCommand(TxInvocationContext ctx, AcquireValidationLocksCommand command) throws Throwable {
        ReadWriteLockManager rwlman = (ReadWriteLockManager) lockManager;
        Object actualKeyInValidation = null;
        try {
            log.debugf("validate transaction [%s] write set %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), command.getWriteSet());
            for(Object k : command.getWriteSet()) {
                if(!rwlman.lockAndRecord(k, ctx)) {
                    Object owner = lockManager.getOwner(k);
                    // if lock cannot be acquired, expose the key itself, not the marshalled value
                    if (k instanceof MarshalledValue) {
                        k = ((MarshalledValue) k).get();
                    }
                    throw new TimeoutException("Unable to acquire lock on key [" + k + "] for requestor [" +
                            ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
                }
                if(!ctx.getLookedUpEntries().containsKey(k)) {
                    actualKeyInValidation = k;
                    ctx.putLookedUpEntry(k, null); //to release later
                }
            }

            log.debugf("validate transaction [%s] read set %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), command.getReadSet());
            for(Object k : command.getReadSet()) {
                if(!rwlman.sharedLockAndRecord(k, ctx)) {
                    Object owner = lockManager.getOwner(k);
                    // if lock cannot be acquired, expose the key itself, not the marshalled value
                    if (k instanceof MarshalledValue) {
                        k = ((MarshalledValue) k).get();
                    }
                    throw new TimeoutException("Unable to acquire lock on key [" + k + "] for requestor [" +
                            ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
                }
                if(!ctx.getLookedUpEntries().containsKey(k)) {
                    actualKeyInValidation = k;
                    ctx.putLookedUpEntry(k, null); //same reason above (release later)
                }
                validateKey(k, command.getVersion());
            }

            actualKeyInValidation = null;
            return invokeNextInterceptor(ctx, command); //it does no need to passes down in the chain
        } catch(Throwable t) {
            //if some exception occurs in mehtod putLookedUpEntry
            if(actualKeyInValidation != null) {
                rwlman.unlock(actualKeyInValidation);
            }
            rwlman.unlock(ctx);
            log.debugf("validation of transaction [%s] fails %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), t.getMessage());
            throw t;
        }
    }

    @Override
    public Object visitTotalOrderPrepareCommand(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        return visitPrepareCommand(ctx, command);
    }

    protected void validateKey(Object key, VersionVC toCompare) {
        if(!dataContainer.validateKey(key, toCompare)) {
            throw new ValidationException("validation of key [" + key + "] failed!");
        }
    }

    private void commitEntry(CacheEntry entry, VersionVC version) {
        if(entry instanceof SerializableEntry) {
            ((SerializableEntry) entry).commit(dataContainer, version);
        } else {
            entry.commit(dataContainer);
        }
    }

    private void cleanupLocks(InvocationContext ctx, boolean commit, VersionVC commitVersion) {
        if (commit) {
            ReversibleOrderedSet<Map.Entry<Object, CacheEntry>> entries = ctx.getLookedUpEntries().entrySet();
            Iterator<Map.Entry<Object, CacheEntry>> it = entries.reverseIterator();
            if (trace) log.tracef("Number of entries in context: %s", entries.size());
            while (it.hasNext()) {
                Map.Entry<Object, CacheEntry> e = it.next();
                CacheEntry entry = e.getValue();
                Object key = e.getKey();
                // could be null with read-committed
                if (entry != null && entry.isChanged()) {
                    commitEntry(entry, commitVersion);
                } else {
                    if (trace) log.tracef("Entry for key %s is null, not calling commitUpdate", key);
                }
            }

            //commitVersion is null when the transaction is readonly
            if(ctx.isInTxScope() && commitVersion != null) {
                ((MultiVersionDataContainer) dataContainer).addNewCommittedTransaction(commitVersion);
            }
            ((ReadWriteLockManager)lockManager).unlockAfterCommit(ctx); //this not call the entry.rollback() (instead of releaseLocks(ctx))

        } else {
            lockManager.releaseLocks(ctx);
        }
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

    /*
    @Override
    public void addTransaction(List<VersionVC> commitVC) {
        ((MultiVersionDataContainer) dataContainer).addNewCommittedTransaction(commitVC);
    }
    */
}
