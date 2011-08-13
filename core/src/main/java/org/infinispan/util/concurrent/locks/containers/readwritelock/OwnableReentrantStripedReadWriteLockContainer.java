package org.infinispan.util.concurrent.locks.containers.readwritelock;

import org.infinispan.context.InvocationContextContainer;
import org.infinispan.util.concurrent.locks.readwritelock.OwnableReentrantReadWriteLock;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author pedro
 *         Date: 09-08-2011
 */
public class OwnableReentrantStripedReadWriteLockContainer extends AbstractStripedReadWriteLockContainer {

    private OwnableReentrantReadWriteLock[] sharedLocks;
    private InvocationContextContainer icc;

    public OwnableReentrantStripedReadWriteLockContainer(int concurrencyLevel, InvocationContextContainer icc) {
        this.icc = icc;
        initLocks(calculateNumberOfSegments(concurrencyLevel));
    }

    @Override
    protected void initLocks(int numLocks) {
        sharedLocks = new OwnableReentrantReadWriteLock[numLocks];
        for(int i = 0; i < numLocks; ++i) {
            sharedLocks[i] = new OwnableReentrantReadWriteLock(icc);
        }
    }

    @Override
    public Lock getSharedLock(Object key) {
        return sharedLocks[hashToIndex(key)].readLock();
    }

    @Override
    public boolean ownsLock(Object key, Object owner) {
        OwnableReentrantReadWriteLock l = sharedLocks[hashToIndex(key)];
        return owner != null && owner.equals(l.getOwner());
    }

    @Override
    public boolean isLocked(Object key) {
        return sharedLocks[hashToIndex(key)].isWriteLock();
    }

    @Override
    public Lock getLock(Object key) {
        return sharedLocks[hashToIndex(key)].writeLock();
    }

    @Override
    public int getNumLocksHeld() {
        int size = 0;
        for(OwnableReentrantReadWriteLock l : sharedLocks) {
            if(l.isReadOrWriteLocked()) {
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
        return "OwnableReentrantStripedReadWriteLockContainer{" +
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
        OwnableReentrantReadWriteLock l = sharedLocks[hashToIndex(key)];
        return owner != null && l.hasReadOrWriteLock(owner);
    }
}
