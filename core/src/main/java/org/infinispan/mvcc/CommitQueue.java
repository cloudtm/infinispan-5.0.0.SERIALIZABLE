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
import sun.tools.tree.ThisExpression;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author pedro
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 */
public class CommitQueue {

    private final static Log log = LogFactory.getLog(CommitQueue.class);

    private VersionVC prepareVC;
    private final ArrayList<ListEntry> commitQueue;
    private CommitInstance commitInvocationInstance;
    private InterceptorChain ic;
    private InvocationContextContainer icc;
    private VersionVCFactory versionVCFactory;

    private Map<String, String> outOfOrderRollback;

    private boolean trace, debug, info;

    public CommitQueue() {
        
        commitQueue = new ArrayList<ListEntry>();
        outOfOrderRollback = new ConcurrentHashMap<String, String>();
    }
    
    
    
   


    private int searchInsertIndexPos(VersionVC vc, int pos) {
        if(commitQueue.isEmpty()) {
            return 0;
        }
        int idx = 0;
        ListIterator<ListEntry> lit = commitQueue.listIterator();
        ListEntry le;
        while(lit.hasNext()) {
            le = lit.next();
            if(le.commitVC.isAfterInPosition(vc, pos)){
            
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
    private int getCommitCode(GlobalTransaction gtx, int pos) {
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
            if(tempCommitVC.equalsInPosition(other.commitVC, pos)){
            
                if(!other.ready) {
                    return -2;
                }
                
                idx++;
            } else {
                break;
            }
        }

        return idx - 1;
    }

   
    
    @Inject
    public void inject(InterceptorChain ic, InvocationContextContainer icc, VersionVCFactory versionVCFactory) {
        this.ic = ic;
        this.icc = icc;
        this.versionVCFactory=versionVCFactory;
    }

    //AFTER THE VersionVCFactory
    @Start(priority = 31)
    public void start() {
        trace = log.isTraceEnabled();
        debug = log.isDebugEnabled();
        info = log.isInfoEnabled();
        
        prepareVC = this.versionVCFactory.createEmptyVersionVC();

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
    public VersionVC addTransaction(GlobalTransaction gtx, VersionVC actualVectorClock, InvocationContext ctx) {

        String stringTx = Util.prettyPrintGlobalTransaction(gtx);


        synchronized (prepareVC) {
            try{

                prepareVC.setToMaximum(actualVectorClock);
                prepareVC.incrementPosition(this.versionVCFactory, this.versionVCFactory.getMyIndex());


                VersionVC prepared = prepareVC.clone();
                ListEntry le = new ListEntry();
                le.gtx = gtx;
                le.commitVC = prepared;
                le.ctx = ctx;

                synchronized (commitQueue) {




                    int idx = searchInsertIndexPos(prepared, this.versionVCFactory.getMyIndex());

                    //log.infof("Adding transaction %s [%s] to queue in position %s. queue state is %s", Util.prettyPrintGlobalTransaction(gtx), prepared, idx, commitQueue.toString());

                    commitQueue.add(idx, le);
                    //commitQueue.notifyAll();





                }
                return prepared;
            }
            catch(CloneNotSupportedException e){
                e.printStackTrace();
                return null;
            }
        }
    }


    public void updateOnlyPrepareVC(VersionVC commitVC){

        synchronized (prepareVC){
            prepareVC.setToMaximum(commitVC);

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

        
        int commitCode=-1;
        
        synchronized (commitQueue) {
            ListEntry toSearch = new ListEntry();
            toSearch.gtx = gtx;
            int idx = commitQueue.indexOf(toSearch);

            if(idx == -1) {
                return ;
            }

            //update the position on the queue
            ListEntry le = commitQueue.get(idx);
            commitQueue.remove(idx);
            le.commitVC = commitVC;
            idx = searchInsertIndexPos(commitVC, this.versionVCFactory.getMyIndex());
            commitQueue.add(idx, le);

            commitQueue.get(0).headStartTs = System.nanoTime();


            if(info){
                log.infof("Update transaction %s position in queue. Final index is %s and commit version is %s",
                        Util.prettyPrintGlobalTransaction(gtx),
                        idx, commitVC);
            }

            le.ready = true;
            commitQueue.notifyAll();

            while(true) {

                commitCode = getCommitCode(gtx, this.versionVCFactory.getMyIndex());

                if(commitCode == -2) { //it is not it turn
                    commitQueue.wait();
                    continue;
                } else if(commitCode == -1) { //already committed
                    commitQueue.remove(le); //We don't really need this line
                    commitQueue.notifyAll();
                    
                    return;
                }

                if(le.headStartTs != 0) {
                	if(info){
                    log.infof("Transaction %s has %s nano seconds in the head of the queue",
                            Util.prettyPrintGlobalTransaction(gtx), System.nanoTime() - le.headStartTs);
                	}
                }

                if(commitCode >= commitQueue.size()) {
                	if(info){
                		log.infof("The commit code received is higher than the size of commit queue (%s > %s)",
                            commitCode, commitQueue.size());
                	}
                    commitCode = commitQueue.size() - 1;
                }

                toCommit.addAll(commitQueue.subList(0, commitCode + 1));
                break;
            }
            if(info){
            log.infof("Transaction %s will apply it write set now. Queue state is %s",
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
        
        if(log.isDebugEnabled() && commitCode>=1){
        	String listVC="";
        	VersionVC current;
        	Iterator<VersionVC> itr=committedTxVersionVC.iterator(); 
        	while(itr.hasNext()){
        		current=itr.next();
        		listVC+=" "+current;
        	}
        	if(debug){
        		log.debug("Batching: "+listVC);
        	}	
        }
        
        VersionVC maxCommittedVC=VersionVC.computeMax(committedTxVersionVC);

        //Insert only one entry in the commitLog. This is the maximum vector clock.
        commitInvocationInstance.addTransaction(maxCommittedVC);

        synchronized (commitQueue) {
            if(commitQueue.removeAll(toCommit)) {
                commitQueue.notifyAll();
            }
            if(!commitQueue.isEmpty()) {
                commitQueue.get(0).headStartTs = System.nanoTime();
            }
        }
    }

    /**
     * remove the transaction (ie. if the transaction rollbacks)
     * @param gtx the transaction identifier
     */
    @SuppressWarnings({"SuspiciousMethodCalls"})
    public void remove(GlobalTransaction gtx, boolean commit) {
        ListEntry toSearch = new ListEntry();
        toSearch.gtx = gtx;

        String stringTx = Util.prettyPrintGlobalTransaction(gtx);
        synchronized (commitQueue) {
            if(trace) {
                log.tracef("Remove transaction %s from queue. Queue is %s",
                        stringTx, commitQueue.toString());
            }
            if(commitQueue.remove(toSearch)) {

                commitQueue.notifyAll();


            }
            else if(!commit){
                //This transaction is not inserted in the queue. This means that this transaction has only read on this node
                //This is not an out of order rollback since we solve this problem in RollbackCommand and PrepareCommand classes.
                //log.warnf("We have a warning here. Removing transaction %s from queue does not succeed.",stringTx);




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
        private volatile long headStartTs = 0;

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
        //void addTransaction(List<VersionVC> commitVC);
    }
}
