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
        if(commitInvocationInstance == null) {
            List<CommandInterceptor> all = ic.getInterceptorsWhichExtend(LockingInterceptor.class);
            log.debugf("interceptors found: %s", all);
            for(CommandInterceptor ci : all) {
                if(ci instanceof CommitInstance) {
                    commitInvocationInstance = (CommitInstance) ci;
                    break;
                }
            }
        }
        if(commitInvocationInstance == null) {
            throw new NullPointerException("commit invocation instance must not be null in serializable mode!!");
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
            if(gtx.isRemote()) {
                le.ctx = ctx;
            }
            synchronized (commitQueue) {
                int idx = searchInsertIndex(prepared);
                log.debugf("added to queue %s in position %s. queue is %s",
                        Util.prettyPrintGlobalTransaction(gtx), idx, commitQueue.toString());
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
     */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    public void updateAndWait(GlobalTransaction gtx, VersionVC commitVC) throws InterruptedException {
        synchronized (prepareVC) {
            prepareVC.setToMaximum(commitVC);
            log.debugf("update transaction %s and prepareVC %s",
                    Util.prettyPrintGlobalTransaction(gtx),
                    prepareVC);
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
                commitQueue.notifyAll();
            }

            log.debugf("update transaction %s and index is %s",
                    Util.prettyPrintGlobalTransaction(gtx),
                    idx);

            if(le.gtx.isRemote()) {
                le.ready = true;
                commitQueue.notifyAll();
                return; //don't wait
            }
            while(idx != 0) {
                log.debugf("wait for my turn... I'm %s and queue state is %s",
                        Util.prettyPrintGlobalTransaction(gtx), commitQueue.toString());
                commitQueue.wait();
                idx = commitQueue.indexOf(toSearch);
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
            log.debugf("remove from queue %s. queue is %s",
                    Util.prettyPrintGlobalTransaction(gtx), commitQueue.toString());
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
            log.debugf("remove first. queue is %s",commitQueue.toString());
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
                    ctx +"}";
        }
    }

    private class ApplyRemoteModificationThread extends Thread {
        private volatile boolean run = false;

        public ApplyRemoteModificationThread() {
            super("ApplyRemoteModificationThread");
        }

        @Override
        public void run() {
            run = true;
            while(run) {
                try {
                    synchronized (commitQueue) {
                        if(commitQueue.isEmpty()) {
                            commitQueue.wait();
                            continue;
                        }
                        ListEntry le = commitQueue.get(0);
                        if(!le.gtx.isRemote() || !le.ready) {
                            commitQueue.wait();
                            continue;
                        }
                        try {
                            icc.resume(le.ctx);
                            log.warnf("commit thread... looked up keys are %s", le.ctx.getLookedUpEntries());
                            commitInvocationInstance.commit(le.ctx, le.commitVC);
                        } finally {
                            icc.suspend();
                            removeFirst();
                        }
                    }
                } catch (InterruptedException e) {
                    //no-op
                } catch (Throwable t) {
                    log.warnf("exception caught in apply commit thread... [%s]", t.getLocalizedMessage());
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
        void commit(InvocationContext ctx, VersionVC commitVersion);
    }
}
