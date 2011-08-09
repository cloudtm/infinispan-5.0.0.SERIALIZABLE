package org.infinispan.util.concurrent.locks.containers.readwritelock;

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author pedro
 *         Date: 09-08-2011
 */
public class ReentrantPerEntryReadWriteLockContainer extends AbstractPerEntryReadWriteLockContainer {


    public ReentrantPerEntryReadWriteLockContainer(int concurrencyLevel) {
        super(concurrencyLevel);
    }

    @Override
    protected ReadWriteLock newLock() {
        return new ReentrantReadWriteLock();
    }

    @Override
    protected boolean isReadOrWriteLocked(ReadWriteLock lock) {
        ReentrantReadWriteLock l = (ReentrantReadWriteLock) lock;
        return l != null && (l.getReadLockCount() > 0 || l.isWriteLocked());
    }

    @Override
    public boolean ownsLock(Object key, Object owner) {
        ReentrantReadWriteLock l = getReentrantReadWriteLock(key);
        return l != null && l.writeLock().isHeldByCurrentThread();
    }

    @Override
    public boolean isLocked(Object key) {
        ReentrantReadWriteLock l = getReentrantReadWriteLock(key);
        return l != null && l.isWriteLocked();
    }

    private ReentrantReadWriteLock getReentrantReadWriteLock(Object key) {
        return (ReentrantReadWriteLock) locks.get(key);
    }

    @Override
    public String toString() {
        return "ReentrantPerEntryReadWriteLockContainer{number of locks="+ locks.size() +"}";
    }
}
