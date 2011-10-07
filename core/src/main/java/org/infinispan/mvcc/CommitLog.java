package org.infinispan.mvcc;


import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author pedro
 *         Date: 25-07-2011
 */
public class CommitLog {
    private static final Log log = LogFactory.getLog(CommitLog.class);

    private final AtomicReference<VersionEntry> chainOfVersion;
    private final VersionVC actual;
    private final Object versionChangeNotifier = new Object();

    private boolean debug;

    public CommitLog() {
        chainOfVersion = new AtomicReference<VersionEntry>(null);
        VersionEntry ve = new VersionEntry();
        ve.version = VersionVC.EMPTY_VERSION;
        actual = new VersionVC();
        chainOfVersion.set(ve);
    }

    @Start
    public void start() {
        if(chainOfVersion.get() == null) {
            VersionEntry ve = new VersionEntry();
            ve.version = VersionVC.EMPTY_VERSION;
            chainOfVersion.compareAndSet(null, ve);
            actual.clean();
        }
        debug = log.isDebugEnabled();
    }

    @Stop
    public void stop() {
        chainOfVersion.set(null);
    }

    public VersionVC getActualVersion() {
        synchronized (actual) {
            return actual.copy();
        }
    }

    public VersionVC getMostRecentLessOrEqualThan(VersionVC other) {
        VersionEntry it = chainOfVersion.get();
        while(it != null) {
            if(it.version == VersionVC.EMPTY_VERSION) {
                return (other != null ? other.copy() : it.version.copy());
            } else if(it.version.isBefore(other)) {
                if(debug) {
                    log.debugf("Get version less or equal than %s. return value is %s",
                            other, it.version);
                }
                return it.version.copy();
            }
            it = it.previous;
        }

        if(debug) {
            log.debugf("Get version less or equal than %s. return value is null",
                    other);
        }
        return null;
    }

    public synchronized void addNewVersion(VersionVC other) {
        VersionEntry actualEntry;
        VersionEntry ve = new VersionEntry();
        ve.version = other.copy();

        do {
            actualEntry = chainOfVersion.get();
            ve.previous = actualEntry;
        } while(!chainOfVersion.compareAndSet(actualEntry, ve));

        synchronized (actual) {
            actual.setToMaximum(other);
            actual.notifyAll();
            if(debug) {
                log.debugf("Added new version[%s] to commit log. actual version is %s",
                        other, actual);
            }
        }
    }

    public synchronized void addNewVersion(List<VersionVC> committedTxVersion) {
        if(committedTxVersion == null || committedTxVersion.isEmpty()) {
            return ;
        }

        VersionVC toUpdateActual = new VersionVC();
        for(VersionVC other : committedTxVersion) {
            VersionEntry actualEntry;
            VersionEntry ve = new VersionEntry();
            ve.version = other.copy();

            do {
                actualEntry = chainOfVersion.get();
                ve.previous = actualEntry;
            } while(!chainOfVersion.compareAndSet(actualEntry, ve));

            toUpdateActual.setToMaximum(other);
        }

        synchronized (actual) {
            actual.setToMaximum(toUpdateActual);
            actual.notifyAll();
            if(debug) {
                log.debugf("Added news versions [%s] to commit log. actual version is %s",
                        committedTxVersion, actual);
            }
        }
    }

    /**
     *
     * @param minVersionVC minimum version
     * @param position minimum version in this position of the vector clock
     * @param timeout timeout in milliseconds
     * @return true if the value is available, false otherwise (timeout)
     * @throws InterruptedException if interrupted
     */
    public boolean waitUntilMinVersionIsGuaranteed(VersionVC minVersionVC, int position, long timeout) throws InterruptedException {
        long minVersion = minVersionVC.get(position);
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
            synchronized (actual) {
                actual.wait(finalTimeout - System.currentTimeMillis());
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
