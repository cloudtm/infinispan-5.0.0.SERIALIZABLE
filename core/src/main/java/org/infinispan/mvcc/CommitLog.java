package org.infinispan.mvcc;


import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author pedro
 *         Date: 25-07-2011
 */
public class CommitLog {
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private VersionEntry actual;

    public CommitLog() {
        actual = new VersionEntry();
        actual.version = new VersionVC();
    }

    public VersionVC getActualVersion() {
        try {
            lock.readLock().lock();
            return actual.version;
        } finally {
            lock.readLock().unlock();
        }
    }

    public VersionVC getMostRecentLessOrEqualThan(VersionVC other) {
        try {
            lock.readLock().lock();
            VersionEntry it = actual;
            while(it != null) {
                if(it.version.isLessOrEquals(other)) {
                    return it.version;
                }
                it = it.previous;
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public void addNewVersion(VersionVC other) {
        try {
            lock.writeLock().lock();
            VersionVC newVersion = other.copy();
            newVersion.setToMaximum(actual.version);
            VersionEntry ve = new VersionEntry();
            ve.version = other;
            ve.previous = actual;
            actual = ve;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private static class VersionEntry {
        private VersionVC version;
        private VersionEntry previous;
    }
}
