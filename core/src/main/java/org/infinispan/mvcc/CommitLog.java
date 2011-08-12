package org.infinispan.mvcc;


import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author pedro
 *         Date: 25-07-2011
 */
public class CommitLog {
    private static final Log log = LogFactory.getLog(CommitLog.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private VersionEntry actual;
    private final AtomicLong prepareCounter = new AtomicLong(0);

    //testing other things...
    public final ReentrantLock commitLock = new ReentrantLock();

    public CommitLog() {
        actual = new VersionEntry();
        actual.version = new VersionVC();
    }

    @Start
    public void start() {
        if(actual == null) {
            actual = new VersionEntry();
            actual.version = new VersionVC();
        }
        prepareCounter.set(0);
    }

    @Stop
    public void stop() {
        actual = null;
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

    public void addNewVersion(VersionVC other, int idx) {
        try {
            lock.writeLock().lock();
            VersionVC newVersion = other.copy();
            newVersion.setToMaximum(actual.version);
            VersionEntry ve = new VersionEntry();
            ve.version = other;
            ve.previous = actual;
            actual = ve;

            long newCounter = actual.version.get(idx);
            if(newCounter < prepareCounter.get()) {
                prepareCounter.set(newCounter);
            }
        } finally {
            log.debugf("added new version to commit log. actual is %s and prepare counter is %s",
                    actual.version, prepareCounter.get());
            lock.writeLock().unlock();
        }
    }

    public long getPrepareCounter() {
        return prepareCounter.incrementAndGet();
    }

    private static class VersionEntry {
        private VersionVC version;
        private VersionEntry previous;
    }
}
