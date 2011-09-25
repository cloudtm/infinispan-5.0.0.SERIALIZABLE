package org.infinispan.mvcc;


import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author pedro
 *         Date: 25-07-2011
 */
public class CommitLog {
    private static final Log log = LogFactory.getLog(CommitLog.class);

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private VersionEntry actual;
    private final Object versionChangeNotifier = new Object();

    private boolean debug;

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
        debug = log.isDebugEnabled();
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
                    if(debug) {
                        log.debugf("Get version less or equal than %s. return value is %s",
                                other, it.version);
                    }
                    return it.version;
                }
                it = it.previous;
            }

            if(debug) {
                log.debugf("Get version less or equal than %s. return value is null",
                        other);
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
            synchronized (versionChangeNotifier) {
                versionChangeNotifier.notifyAll();
            }
        } finally {
            if(debug) {
                log.debugf("Added new version to commit log. actual version is %s",
                        actual.version);
            }
            lock.writeLock().unlock();
        }
    }

    /**
     *
     * @param minVersion minimum version
     * @param position minimum version in this position of the vector clock
     * @param timeout timeout in milliseconds
     * @return true if the value is available, false otherwise (timeout)
     * @throws InterruptedException if interrupted
     */
    public boolean waitUntilMinVersionIsGuaranteed(long minVersion, int position, long timeout) throws InterruptedException {
        if(minVersion <= 0) {
            if(debug) {
                log.debugf("Wait until min version but min version is less or equals than zero");
            }
            return true;
        }

        long finalTimeout = System.currentTimeMillis() + timeout;
        do {
            VersionVC version = getActualVersion();
            if(debug) {
                log.debugf("Wait until min version. actual version: %s, min version %s, position: %s",
                        version, minVersion, position);
            }
            if(version.get(position) >= minVersion) {
                return true;
            }
            synchronized (versionChangeNotifier) {
                versionChangeNotifier.wait(finalTimeout - System.currentTimeMillis());
            }
        } while(System.currentTimeMillis() < finalTimeout);

        VersionVC version = getActualVersion();

        if(debug) {
            log.debugf("Wait until min version. actual version: %s, min version %s, position: %s",
                    version, minVersion, position);
        }

        return version.get(position) >= minVersion;
    }

    private static class VersionEntry {
        private VersionVC version;
        private VersionEntry previous;
    }
}
