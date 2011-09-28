package org.infinispan.container;

import org.infinispan.config.Configuration;
import org.infinispan.container.entries.*;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.marshall.MarshalledValue;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.concurrent.locks.readwritelock.ReadWriteLockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * @author pruivo
 * Date: 10/08/11
 */
public class MultiVersionEntryFactoryImpl implements EntryFactory {

    private DataContainer container;
    private ReadWriteLockManager lockManager;
    private Configuration configuration;
    private CacheNotifier notifier;

    private static final Log log = LogFactory.getLog(EntryFactoryImpl.class);
    private static final boolean trace = log.isTraceEnabled();

    private MVCCEntry createWrappedEntry(Object key, Object value, boolean isForInsert, boolean forRemoval,
                                         long lifespan) {
        if (value == null && !isForInsert) {
            return forRemoval ? new NullMarkerEntryForRemoval(key) : NullMarkerEntry.getInstance();
        }

        return new SerializableEntry(key, value, lifespan);
    }


    private MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, InternalCacheEntry entry,
                                          boolean createIfAbsent, boolean forceLockIfAbsent,
                                          boolean alreadyLocked, boolean forRemoval,
                                          boolean undeleteIfNeeded) throws InterruptedException {

        CacheEntry cacheEntry = ctx.lookupEntry(key);
        MVCCEntry mvccEntry = null;

        if (createIfAbsent && cacheEntry != null && cacheEntry.isNull()) {
            cacheEntry = null;
        }

        // exists in context!  Just acquire lock if needed, and wrap.
        if (cacheEntry != null) {
            if (trace) {
                log.trace("Exists in context.");
            }

            if (cacheEntry instanceof MVCCEntry && (!forRemoval || !(cacheEntry instanceof NullMarkerEntry))) {
                mvccEntry = (MVCCEntry) cacheEntry;
            } else {
                // this is a read-only entry that needs to be copied to a proper read-write entry!!
                mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), false, forRemoval, cacheEntry.getLifespan());
                cacheEntry = mvccEntry;
                ctx.putLookedUpEntry(key, cacheEntry);
            }

            // create a copy of the underlying entry
            mvccEntry.copyForUpdate(container, false, false);

            if (cacheEntry.isRemoved() && createIfAbsent && undeleteIfNeeded) {
                if (trace) {
                    log.trace("Entry is deleted in current scope.  Need to un-delete.");
                }
                mvccEntry.setRemoved(false);
                mvccEntry.setValid(true);
            }

            return mvccEntry;
        } else {
            // else, fetch from dataContainer or used passed entry.
            cacheEntry = entry != null ? entry : container.get(key);

            if (cacheEntry != null) {
                if (trace) {
                    log.trace("Retrieved from container.");
                }

                mvccEntry = createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
                ctx.putLookedUpEntry(key, mvccEntry);
                mvccEntry.copyForUpdate(container, false, false);
            } else if (createIfAbsent) {
                // this is the *only* point where new entries can be created!!
                if (trace) {
                    log.trace("Creating new entry.");
                }

                notifier.notifyCacheEntryCreated(key, true, ctx);

                mvccEntry = createWrappedEntry(key, null, true, false, -1);
                mvccEntry.setCreated(true);
                ctx.putLookedUpEntry(key, mvccEntry);
                mvccEntry.copyForUpdate(container, false, false);
                notifier.notifyCacheEntryCreated(key, false, ctx);
            }
        }

        // see if we need to force the lock on nonexistent entries.
        if (mvccEntry == null && forceLockIfAbsent) {
            ctx.putLookedUpEntry(key, null);
        }

        return mvccEntry;
    }

    @Inject
    public void injectDependencies(DataContainer dataContainer, ReadWriteLockManager lockManager,
                                   Configuration configuration, CacheNotifier notifier) {
        this.container = dataContainer;
        this.configuration = configuration;
        this.lockManager = lockManager;
        this.notifier = notifier;
    }

    @Override
    public void releaseLock(Object key) {
        lockManager.unlock(key);
    }

    @Override
    public boolean acquireLock(InvocationContext ctx, Object key) throws InterruptedException, TimeoutException {
        boolean shouldSkipLocking = ctx.hasFlag(Flag.SKIP_LOCKING);

        if (!ctx.hasLockedKey(key) && !shouldSkipLocking) {
            if (lockManager.lockAndRecord(key, ctx)) {
                return true;
            } else {
                Object owner = lockManager.getOwner(key);
                if (key instanceof MarshalledValue) {
                    key = ((MarshalledValue) key).get();
                }
                throw new TimeoutException("Unable to acquire lock after [" +
                        Util.prettyPrintTime(lockManager.getLockAcquisitionTimeout(ctx)) + "] on key [" + key +
                        "] for requestor [" + ctx.getLockOwner() + "]! Lock held by [" + owner + "]");
            }
        } else {
            if (trace) {
                if (shouldSkipLocking) {
                    log.trace("SKIP_LOCKING flag used!");
                } else{
                    log.trace("Already own lock for entry");
                }
            }
        }

        return false;
    }

    @Override
    public MVCCEntry wrapEntryForWriting(InvocationContext ctx, Object key, boolean createIfAbsent,
                                         boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval,
                                         boolean undeleteIfNeeded) throws InterruptedException {
        return wrapEntryForWriting(ctx, key, null, createIfAbsent, forceLockIfAbsent, alreadyLocked, forRemoval,
                undeleteIfNeeded);
    }

    @Override
    public MVCCEntry wrapEntryForWriting(InvocationContext ctx, InternalCacheEntry entry, boolean createIfAbsent,
                                         boolean forceLockIfAbsent, boolean alreadyLocked, boolean forRemoval,
                                         boolean undeleteIfNeeded) throws InterruptedException {
        return wrapEntryForWriting(ctx, entry.getKey(), entry, createIfAbsent, forceLockIfAbsent, alreadyLocked,
                forRemoval, undeleteIfNeeded);
    }

    @Override
    public CacheEntry wrapEntryForReading(InvocationContext ctx, Object key) throws InterruptedException {
        CacheEntry cacheEntry;
        if ((cacheEntry = ctx.lookupEntry(key)) == null) {
            if (trace) {
                log.tracef("Key %s is not in context, fetching from container.", key);
            }

            if (ctx.isInTxScope() || ctx.readBasedOnVersion()) {

                VersionVC maxToRead = ctx.calculateVersionToRead();

                InternalMVCCEntry ime = container.get(key, maxToRead);
                cacheEntry = ime.getValue();

                MVCCEntry mvccEntry = cacheEntry == null ?
                        createWrappedEntry(key, null, false, false, -1) :
                        createWrappedEntry(key, cacheEntry.getValue(), false, false, cacheEntry.getLifespan());
                if (mvccEntry != null) {
                    ctx.addReadKey(key,ime);
                    ctx.putLookedUpEntry(key, mvccEntry);
                }

                return mvccEntry;
            } else {
                cacheEntry = container.get(key);
                if(cacheEntry != null) {
                    ctx.putLookedUpEntry(key, cacheEntry);
                }
                return cacheEntry;
            }
        } else {
            if (trace) {
                log.trace("Key is already in context");
            }
            return cacheEntry;
        }
    }
}
