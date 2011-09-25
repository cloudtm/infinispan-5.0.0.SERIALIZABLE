package org.infinispan.mvcc;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.LockingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

/**
 * @author pedro
 *         Date: 25-08-2011
 */
public class CommitQueue {

    private final static Log log = LogFactory.getLog(CommitQueue.class);

    private final VersionVC prepareVC;
    private final ArrayList<ListEntry> commitQueue;
    private CommitInstance commitInvocationInstance;
    private final ApplyRemoteModificationThread applyThread;
    private InterceptorChain ic;
    private InvocationContextContainer icc;

    private boolean trace, debug;

    public CommitQueue() {
        prepareVC = new VersionVC();
        commitQueue = new ArrayList<ListEntry>();
        applyThread = new ApplyRemoteModificationThread();
    }

    private int searchInsertIndex(VersionVC vc) {
        if(commitQueue.isEmpty()) {
            return 0;
        }
        int idx = 0;
        ListIterator<ListEntry> lit = commitQueue.listIterator();
        ListEntry le;
        while(lit.hasNext()) {
            le = lit.next();
            if(le.commitVC.isGreaterThan(vc)) {
                return idx;
            }
            idx++;
        }
        return idx;
    }

    @Inject
    public void inject(InterceptorChain ic, InvocationContextContainer icc) {
        this.ic = ic;
        this.icc = icc;
    }

    @Start
    public void start() {
        trace = log.isTraceEnabled();
        debug = log.isDebugEnabled();

        if(commitInvocationInstance == null) {
            List<CommandInterceptor> all = ic.getInterceptorsWhichExtend(LockingInterceptor.class);
            if(log.isInfoEnabled()) {
                log.infof("Starting Commit Queue Component. Searching interceptors with interface CommitInstance. " +
                        "Found: %s", all);
            }
            for(CommandInterceptor ci : all) {
                if(ci instanceof CommitInstance) {
                    if(debug) {
                        log.debugf("Interceptor implementing CommitInstance found! It is %s", ci);
                    }
                    commitInvocationInstance = (CommitInstance) ci;
                    break;
                }
            }
        }
        if(commitInvocationInstance == null) {
            throw new NullPointerException("Commit Invocation Instance must not be null in serializable mode.");
        }
        if(!applyThread.run) {
            applyThread.start();
        }
    }

    @Stop
    public void stop() {
        if(applyThread.run) {
            applyThread.interrupt();
        }
    }

    /**
     * add a transaction to the queue. A temporary commit vector clock is associated
     * and with it, it order the transactions. this commit vector clocks is returned.
     * @param gtx the transaction identifier
     * @param actualVectorClock the vector clock constructed while executing the transaction
     * @param ctx the context
     * @param positions the positions to be updated
     * @return the prepare vector clock
     */
    public VersionVC addTransaction(GlobalTransaction gtx, VersionVC actualVectorClock, InvocationContext ctx, Integer... positions) {
        synchronized (prepareVC) {
            prepareVC.setToMaximum(actualVectorClock);
            prepareVC.incrementPositions(positions);
            VersionVC prepared = prepareVC.copy();
            ListEntry le = new ListEntry();
            le.gtx = gtx;
            le.commitVC = prepared;
            le.ctx = ctx;

            synchronized (commitQueue) {
                int idx = searchInsertIndex(prepared);
                if(debug) {
                    log.debugf("Adding transaction %s [%s] to queue in position %s. queue state is %s",
                            Util.prettyPrintGlobalTransaction(gtx), prepared, idx, commitQueue.toString());
                }
                commitQueue.add(idx, le);
                commitQueue.notifyAll();
            }
            return prepared;
        }
    }


    /**
     * updates the position on the queue, mark the transaction as ready to commit and puts the final vector clock
     * (commit vector clock).
     * This method only returns when the transaction arrives to the top of the queue
     * (invoked when the transaction commits)
     * @param gtx the transaction identifier
     * @param commitVC the commit vector clock
     * @throws InterruptedException if it is interrupted
     * @return true if the modification was already applied
     */
    public boolean updateAndWait(GlobalTransaction gtx, VersionVC commitVC) throws InterruptedException {

        synchronized (prepareVC) {
            prepareVC.setToMaximum(commitVC);
            if(debug) {
                log.debugf("Update prepare vector clock to %s",
                        prepareVC);
            }
        }

        ListEntry toSearch = new ListEntry();
        toSearch.gtx = gtx;
        synchronized (commitQueue) {
            int idx = commitQueue.indexOf(toSearch);
            ListEntry le = commitQueue.get(idx);
            if(!commitVC.isEquals(le.commitVC)) {
                commitQueue.remove(idx);
                le.commitVC = commitVC;
                idx = searchInsertIndex(commitVC);
                commitQueue.add(idx, le);
            }

            if(debug) {
                log.debugf("Update transaction %s position in queue. Final index is %s and commit version is %s",
                        Util.prettyPrintGlobalTransaction(gtx),
                        idx, commitVC);
            }

            le.ready = true;
            commitQueue.notifyAll();

            if(le.gtx.isRemote()) {
                if(trace) {
                    log.tracef("Transaction [%s] is remote... don't wait", Util.prettyPrintGlobalTransaction(gtx));
                }
                return false; //don't wait (don't care about the return value)
            }

            while(true) {

                if(idx != 0) {
                    if(debug) {
                        log.debugf("Transaction [%s] is not on head of the queue. Waiting for its turn. Queue is %s",
                                Util.prettyPrintGlobalTransaction(gtx), commitQueue.toString());
                    }
                    commitQueue.wait();
                    idx = commitQueue.indexOf(toSearch);
                } else {
                    if(debug) {
                        log.debugf("Transaction [%s] is on head of the queue. Queue is %s",
                                Util.prettyPrintGlobalTransaction(gtx), commitQueue.toString());
                    }
                }

                if(idx == 0 && commitQueue.size() > 1) {
                    ListEntry other = commitQueue.get(1);
                    if(debug) {
                        log.debugf("Compare entries for batching. %s and %s", le, other);
                    }
                    if(other.commitVC.isEquals(le.commitVC)) {
                        //both has the same commit vector clock... we need to wrap this transaction in one
                        if(!other.ready) {
                            if(trace) {
                                log.tracef("compare entries %s and %s ==> same vector clock, but other is not ready",
                                        le, other);
                            }
                            idx = -1; //other is not ready... the vector clock can change. we must sure
                            // if they are different or not
                        } else {
                            if(trace) {
                                log.tracef("compare entries %s and %s ==> same vector clock, and other is ready",
                                        le, other);
                            }
                            //the other is ready and with the same vector clock... grab the modification and join to
                            //this transaction
                            le.ctx.putLookedUpEntries(other.ctx.getLookedUpEntries());
                            other.applied = true;
                        }
                    }
                }

                if(idx == 0) {
                    if(debug) {
                        log.debugf("Transaction [%s] is on head of the queue. The modification %s applied! Queue is %s",
                                Util.prettyPrintGlobalTransaction(gtx),
                                (le.applied ? "are" : "are not"),
                                commitQueue.toString());
                    }
                    return le.applied;
                }
            }
        }

    }

    /**
     * remove the transaction (ie. if the transaction rollbacks)
     * @param gtx the transaction identifier
     */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    public void remove(GlobalTransaction gtx) {
        ListEntry toSearch = new ListEntry();
        toSearch.gtx = gtx;
        synchronized (commitQueue) {
            if(trace) {
                log.tracef("Remove transaction %s from queue. Queue is %s",
                        Util.prettyPrintGlobalTransaction(gtx), commitQueue.toString());
            }
            if(commitQueue.remove(toSearch)) {
                commitQueue.notifyAll();
            }
        }
    }

    /**
     * removes the first element of the queue
     */
    public void removeFirst() {
        synchronized (commitQueue) {
            if(trace) {
                log.tracef("Remove first transaction from queue. Queue is %s", commitQueue.toString());
            }
            commitQueue.remove(0);
            commitQueue.notifyAll();
        }
    }

    /**
     * removes all the elements
     */
    public void clear() {
        synchronized (commitQueue) {
            commitQueue.clear();
            commitQueue.notifyAll();
        }
    }

    private static class ListEntry {
        private GlobalTransaction gtx;
        private VersionVC commitVC;
        private InvocationContext ctx;
        private volatile boolean ready = false;
        private volatile boolean applied = false;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null) return false;

            if(getClass() == o.getClass()) {
                ListEntry listEntry = (ListEntry) o;
                return gtx != null ? gtx.equals(listEntry.gtx) : listEntry.gtx == null;
            }

            return false;
        }

        @Override
        public int hashCode() {
            return gtx != null ? gtx.hashCode() : 0;
        }

        @Override
        public String toString() {
            return "ListEntry{gtx=" + Util.prettyPrintGlobalTransaction(gtx) + ",commitVC=" + commitVC + ",ctx=" +
                    ctx + ",ready?=" + ready + ",applied?=" + applied + "}";
        }
    }

    private class ApplyRemoteModificationThread extends Thread {
        private volatile boolean run = false;

        public ApplyRemoteModificationThread() {
            super("Apply-Remote-Modification-Thread");
        }

        @Override
        public void run() {
            run = true;
            while(run) {
                try {
                    ListEntry le;
                    synchronized (commitQueue) {
                        if(commitQueue.isEmpty()) {
                            if(trace) {
                                log.tracef("Commit Queue is empty. waiting for something...");
                            }
                            commitQueue.wait();
                            continue;
                        }
                        le = commitQueue.get(0);
                        if(!le.gtx.isRemote() || !le.ready) {
                            if(trace) {
                                log.tracef("Head of commit queue [%s] is local or it is not ready. " +
                                        "waiting for something...", le);
                            }
                            commitQueue.wait();
                            continue;
                        }
                    }

                    try {
                        icc.resume(le.ctx);
                        if(debug) {
                            log.debugf("Commit modifications for transaction %s",
                                    Util.prettyPrintGlobalTransaction(le.gtx));
                        }
                        commitInvocationInstance.commit(le.ctx, le.commitVC, le.applied);
                    } finally {
                        icc.suspend();
                        removeFirst();
                    }
                } catch (InterruptedException e) {
                    log.warnf("Interrupted Exception caught on Apply-Remote-Modification-Thread");
                    //no-op
                } catch (Throwable t) {
                    log.warnf("Exception caught on Apply-Remote-Modification-Thread [%s]", t.getLocalizedMessage());
                }
            }
        }

        @Override
        public void interrupt() {
            run = false;
            super.interrupt();
        }
    }

    public static interface CommitInstance {
        void commit(InvocationContext ctx, VersionVC commitVersion, boolean applied);
    }
}
