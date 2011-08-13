package org.infinispan.util.concurrent.locks.containers.readwritelock;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author pedro
 *         Date: 09-08-2011
 */
public class ReentrantStripedReadWriteLockContainer extends AbstractStripedReadWriteLockContainer {

    private ReentrantReadWriteLock[] sharedLocks;

    public ReentrantStripedReadWriteLockContainer(int concurrencyLevel) {
        initLocks(calculateNumberOfSegments(concurrencyLevel));
    }

    @Override
    protected void initLocks(int numLocks) {
        sharedLocks = new ReentrantReadWriteLock[numLocks];
        for(int i = 0; i < numLocks; ++i) {
            sharedLocks[i] = new ReentrantReadWriteLock();
        }
    }

    @Override
    public Lock getSharedLock(Object key) {
        return sharedLocks[hashToIndex(key)].readLock();
    }

    @Override
    public boolean ownsLock(Object key, Object owner) {
        ReentrantReadWriteLock lock = sharedLocks[hashToIndex(key)];
        return lock.isWriteLockedByCurrentThread();
    }

    @Override
    public boolean isLocked(Object key) {
        ReentrantReadWriteLock lock = sharedLocks[hashToIndex(key)];
        return lock.isWriteLocked();
    }

    @Override
    public Lock getLock(Object key) {
        return sharedLocks[hashToIndex(key)].writeLock();
    }

    @Override
    public int getNumLocksHeld() {
        int size = 0;
        for (ReentrantReadWriteLock l : sharedLocks) {
            if (l.getReadLockCount() > 0 || l.isWriteLocked()) {
                size++;
            }
        }
        return size;
    }

    @Override
    public int size() {
        return sharedLocks.length;
    }

    @Override
    public String toString() {
        return "ReentrantStripedReadWriteLockContainer{" +
                "sharedLocks=" + (sharedLocks == null ? null : Arrays.asList(sharedLocks)) +
                '}';
    }

    @Override
    public void clear() {
        sharedLocks = null;
    }

    @Override
    public ReadWriteLock getReadWriteLock(Object key) {
        return sharedLocks[hashToIndex(key)];
    }

    @Override
    public boolean ownsReadOrWriteLock(Object owner, Object key) {
        ReentrantReadWriteLock l = sharedLocks[hashToIndex(key)];
        return l != null && (l.isWriteLockedByCurrentThread() || l.getReadLockCount() != 0);
    }
}
