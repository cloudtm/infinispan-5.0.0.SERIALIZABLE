package org.infinispan.util.concurrent.locks.containers.readwritelock;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;

import static org.infinispan.util.Util.safeRelease;

/**
 * @author pedro
 *         Date: 09-08-2011
 */
public abstract class AbstractStripedReadWriteLockContainer implements ReadWriteLockContainer {

    private int lockSegmentMask;
    private int lockSegmentShift;

    protected abstract void initLocks(int numLocks);

    protected final int calculateNumberOfSegments(int concurrencyLevel) {
        int tempLockSegShift = 0;
        int numLocks = 1;
        while (numLocks < concurrencyLevel) {
            ++tempLockSegShift;
            numLocks <<= 1;
        }
        lockSegmentShift = 32 - tempLockSegShift;
        lockSegmentMask = numLocks - 1;
        return numLocks;
    }

    protected final int hashToIndex(Object object) {
        return (hash(object) >>> lockSegmentShift) & lockSegmentMask;
    }

    /**
     * Returns a hash code for non-null Object x. Uses the same hash code spreader as most other java.util hash tables,
     * except that this uses the string representation of the object passed in.
     *
     * @param object the object serving as a key
     * @return the hash code
     */
    protected final int hash(Object object) {
        int h = object.hashCode();
        h += ~(h << 9);
        h ^= (h >>> 14);
        h += (h << 4);
        h ^= (h >>> 10);
        return h;

    }

    public Lock acquireLock(Object key, long timeout, TimeUnit unit) throws InterruptedException {
        Lock lock = getLock(key);
        boolean locked;
        try {
            locked = lock.tryLock(timeout, unit);
        } catch (InterruptedException ie) {
            safeRelease(lock);
            throw ie;
        } catch (Throwable th) {
            safeRelease(lock);
            locked = false;
        }
        return locked ? lock : null;
    }

    public void releaseLock(Object key) {
        final Lock lock = getLock(key);
        try {
            lock.unlock();
        } catch (IllegalMonitorStateException imse) {
            //no-op
        }
    }

    public int getLockId(Object key) {
        return hashToIndex(key);
    }

    @Override
    public Lock acquireSharedLock(Object key, long timeout, TimeUnit unit) throws InterruptedException {
        Lock lock = getSharedLock(key);
        boolean locked;
        try {
            locked = lock.tryLock(timeout, unit);
        } catch (InterruptedException ie) {
            safeRelease(lock);
            throw ie;
        } catch (Throwable th) {
            safeRelease(lock);
            locked = false;
        }
        return locked ? lock : null;
    }

    @Override
    public void releaseSharedLock(Object key) {
        final Lock lock = getSharedLock(key);
        try {
            lock.unlock();
        } catch (IllegalMonitorStateException imse) {
            //no-op
        }
    }
}
