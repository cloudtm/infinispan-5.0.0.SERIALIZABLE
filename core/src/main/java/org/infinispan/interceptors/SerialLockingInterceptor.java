package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.container.MultiVersionDataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.SerializableEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.ReversibleOrderedSet;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.locks.readwritelock.ReadWriteLockManager;

import java.util.Iterator;
import java.util.Map;

/**
 * @author pedro
 *         Date: 25-08-2011
 */
public class SerialLockingInterceptor extends LockingInterceptor {

    private CommitQueue commitQueue;

    @Inject
    public void inject(CommitQueue commitQueue) {
        this.commitQueue = commitQueue;
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        try {
            return invokeNextInterceptor(ctx, command);
        } finally {
            if (ctx.isInTxScope()) {
                try {
                    commitQueue.updateAndWait(command.getGlobalTransaction(), command.getCommitVersion());
                    cleanupLocks(ctx, true, command.getCommitVersion());
                    commitQueue.removeFirst();
                } finally {
                    commitQueue.remove(command.getGlobalTransaction());
                }
            } else {
                throw new IllegalStateException("Attempting to do a commit or rollback but there is no transactional context in scope. " + ctx);
            }
        }
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        commitQueue.remove(command.getGlobalTransaction());
        return super.visitRollbackCommand(ctx, command);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object visitAcquireValidationLocksCommand(TxInvocationContext ctx, AcquireValidationLocksCommand command) throws Throwable {
        ReadWriteLockManager rwlman = (ReadWriteLockManager) lockManager;
        Object actualKeyInValidation = null;
        try {
            log.debugf("validate transaction [%s] write set %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), command.getWriteSet());
            for(Object k : command.getWriteSet()) {
                rwlman.lockAndRecord(k, ctx);
                if(!ctx.getLookedUpEntries().containsKey(k)) {
                    actualKeyInValidation = k;
                    ctx.putLookedUpEntry(k, null); //to release later
                }
            }

            log.debugf("validate transaction [%s] read set %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), command.getReadSet());
            for(Object k : command.getReadSet()) {
                rwlman.sharedLockAndRecord(k, ctx);
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

    protected void validateKey(Object key, VersionVC toCompare) {
        if(!dataContainer.validateKey(key, 0, toCompare.get(0))) {
            throw new CacheException("validation of key [" + key + "] failed!");
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
}
