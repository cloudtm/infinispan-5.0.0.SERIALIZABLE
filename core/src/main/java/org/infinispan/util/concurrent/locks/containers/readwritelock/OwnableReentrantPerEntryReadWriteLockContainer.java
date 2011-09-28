package org.infinispan.util.concurrent.locks.containers.readwritelock;

import org.infinispan.context.InvocationContextContainer;
import org.infinispan.util.concurrent.locks.readwritelock.OwnableReentrantReadWriteLock;

import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author pedro
 *         Date: 09-08-2011
 */
public class OwnableReentrantPerEntryReadWriteLockContainer extends AbstractPerEntryReadWriteLockContainer {

    private InvocationContextContainer icc;

    public OwnableReentrantPerEntryReadWriteLockContainer(int concurrencyLevel, InvocationContextContainer icc) {
        super(concurrencyLevel);
        this.icc = icc;
    }

    @Override
    protected ReadWriteLock newLock() {
        return new OwnableReentrantReadWriteLock(icc);
    }

    @Override
    protected boolean isReadOrWriteLocked(ReadWriteLock lock) {
        OwnableReentrantReadWriteLock l = (OwnableReentrantReadWriteLock) lock;
        return l != null && l.isReadOrWriteLocked();
    }

    @Override
    public boolean ownsLock(Object key, Object owner) {
        OwnableReentrantReadWriteLock l = getOwnableReentrantReadWriteLock(key);
        return owner != null && owner.equals(l.getOwner());
    }

    @Override
    public boolean ownsReadOrWriteLock(Object owner, Object key) {
        OwnableReentrantReadWriteLock l = getOwnableReentrantReadWriteLock(key);
        return l != null && l.hasReadOrWriteLock(owner);
    }

    @Override
    public boolean isLocked(Object key) {
        OwnableReentrantReadWriteLock l = getOwnableReentrantReadWriteLock(key);
        return l != null && l.isWriteLock();
    }

    private OwnableReentrantReadWriteLock getOwnableReentrantReadWriteLock(Object key) {
        return (OwnableReentrantReadWriteLock) locks.get(key);
    }

    @Override
    public String toString() {
        return "OwnableReentrantPerEntryReadWriteLockContainer{number of locks="+ locks.size() +"}";
    }
}
