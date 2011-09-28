package org.infinispan.mvcc;

import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.LockingInterceptor;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.LinkedList;
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
    private InterceptorChain ic;
    private InvocationContextContainer icc;

    private boolean trace, debug;

    public CommitQueue() {
        prepareVC = new VersionVC();
        commitQueue = new ArrayList<ListEntry>();
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
            if(le.commitVC.isAfter(vc)) {
                return idx;
            }
            idx++;
        }
        return idx;
    }

    //a transaction can commit if it is ready, it is on head of the queue and
    //  the following transactions has a version higher than this one
    //if the following transactions has the same vector clock, then their must be ready to commit
    //  and their modifications will be batched with this transaction

    //return values:
    //   -2: not ready to commit (must wait)
    //   -1: it was already committed (can be removed)
    //    n (>=0): it is ready to commit and commits the 'n'th following txs

    //WARNING: it is assumed that this is called inside a synchronized block!!
    private int getCommitCode(GlobalTransaction gtx) {
        ListEntry toSearch = new ListEntry();
        toSearch.gtx = gtx;
        int idx = commitQueue.indexOf(toSearch);
        if(idx < 0) {
            return -1;
        } else if(idx > 0) {
            return -2;
        }

        toSearch = commitQueue.get(0);

        if(toSearch.applied) {
            return -1;
        }

        VersionVC tempCommitVC = toSearch.commitVC;

        int queueSize = commitQueue.size();
        idx = 1;

        while(idx < queueSize) {
            ListEntry other = commitQueue.get(idx);
            if(tempCommitVC.isEquals(other.commitVC)) {
                if(!other.ready) {
                    return -2;
                }
                tempCommitVC.setToMaximum(other.commitVC);
                idx++;
            } else {
                break;
            }
        }

        return idx - 1;
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
     */
    //v2: commits the keys too!!
    public void updateAndWait(GlobalTransaction gtx, VersionVC commitVC) throws InterruptedException {

        synchronized (prepareVC) {
            prepareVC.setToMaximum(commitVC);
            if(debug) {
                log.debugf("Update prepare vector clock to %s",
                        prepareVC);
            }
        }
        List<ListEntry> toCommit = new LinkedList<ListEntry>();

        synchronized (commitQueue) {
            ListEntry toSearch = new ListEntry();
            toSearch.gtx = gtx;
            int idx = commitQueue.indexOf(toSearch);

            if(idx == -1) {
                return ;
            }

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

            while(true) {

                int commitCode = getCommitCode(gtx);

                if(commitCode == -2) { //it is not it turn
                    commitQueue.wait();
                    continue;
                } else if(commitCode == -1) { //already committed
                    if(commitQueue.remove(le)) {
                        commitQueue.notifyAll();
                    }
                    return;
                }

                if(commitCode >= commitQueue.size()) {
                    log.warnf("The commit code received is higher than the size of commit queue (%s > %s)",
                            commitCode, commitQueue.size());
                    commitCode = commitQueue.size() - 1;
                }

                toCommit.addAll(commitQueue.subList(0, commitCode + 1));
                break;
            }
            if(debug) {
                log.debugf("Transaction %s will apply it write set now. Queue state is %s",
                        Util.prettyPrintGlobalTransaction(le.gtx), commitQueue.toString());
            }
        }

        if(debug) {
            log.debugf("This thread will aplly the write set of " + toCommit);
        }

        List<VersionVC> committedTxVersionVC = new LinkedList<VersionVC>();
        InvocationContext thisCtx = icc.suspend();
        for(ListEntry le : toCommit) {
            icc.resume(le.ctx);
            commitInvocationInstance.commit(le.ctx, le.commitVC);
            icc.suspend();
            le.applied = true;
            committedTxVersionVC.add(le.commitVC);
        }
        icc.resume(thisCtx);

        commitInvocationInstance.addTransaction(committedTxVersionVC);

        synchronized (commitQueue) {
            if(commitQueue.removeAll(toCommit)) {
                commitQueue.notifyAll();
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

    public static interface CommitInstance {
        void commit(InvocationContext ctx, VersionVC commitVersion);
        void addTransaction(VersionVC commitVC);
        void addTransaction(List<VersionVC> commitVC);
    }
}
