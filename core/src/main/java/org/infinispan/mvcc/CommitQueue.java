package org.infinispan.mvcc;

import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.ArrayList;
import java.util.ListIterator;

/**
 * @author pedro
 *         Date: 25-08-2011
 */
public class CommitQueue {

    private final static Log log = LogFactory.getLog(CommitQueue.class);

    private final VersionVC prepareVC;
    private final ArrayList<ListEntry> commitQueue;

    public CommitQueue() {
        prepareVC = new VersionVC();
        commitQueue = new ArrayList<ListEntry>();
    }

    private int searchInsertIndex(int lastIdx, VersionVC vc) {
        if(commitQueue.isEmpty()) {
            return 0;
        }
        int idx = lastIdx;
        ListIterator<ListEntry> lit = commitQueue.listIterator(lastIdx);
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

    /**
     * add a transaction to the queue. A temporary commit vector clock is associated
     * and with it, it order the transactions. this commit vector clocks is returned.
     * @param gtx the transaction identifier
     * @param actualVectorClock the vector clock constructed while executing the transaction
     * @param positions the positions to be updated
     * @return the prepare vector clock
     */
    public VersionVC addTransaction(GlobalTransaction gtx, VersionVC actualVectorClock, Integer... positions) {
        synchronized (prepareVC) {
            prepareVC.setToMaximum(actualVectorClock);
            prepareVC.incrementPositions(positions);
            VersionVC prepared = prepareVC.copy();
            ListEntry le = new ListEntry();
            le.gtx = gtx;
            le.commitVC = prepared;
            synchronized (commitQueue) {
                int idx = searchInsertIndex(0, prepared);
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
        }

        ListEntry toSearch = new ListEntry();
        toSearch.gtx = gtx;
        synchronized (commitQueue) {
            int idx = commitQueue.indexOf(toSearch);
            ListEntry le = commitQueue.get(idx);
            if(!commitVC.isEquals(le.commitVC)) {
                commitQueue.remove(idx);
                le.commitVC = commitVC;
                idx = searchInsertIndex(idx, commitVC);
                log.debugf("added to queue %s in position %s. queue is %s",
                        Util.prettyPrintGlobalTransaction(gtx), idx, commitQueue.toString());
                commitQueue.add(idx, le);
                commitQueue.notifyAll();
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
            return "[ListEntry gtx=" + Util.prettyPrintGlobalTransaction(gtx) + ",commitVC=" + commitVC + "]";
        }
    }
}
