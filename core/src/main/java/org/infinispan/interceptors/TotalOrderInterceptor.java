package org.infinispan.interceptors;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.totalorder.TotalOrderTransactionManager;
import org.infinispan.totalorder.WriteSkewException;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionLog;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;

import java.util.*;
import java.util.concurrent.*;

/**
 * @author pedro
 *         Date: 16-06-2011
 */
public class TotalOrderInterceptor extends CommandInterceptor{
    private final ConcurrentMap<Object, CountDownLatch> keysInValidation = new ConcurrentHashMap<Object, CountDownLatch>();
    protected TotalOrderTransactionManager totman;
    private ExecutorService certifyPool;
    private boolean multithreadCertificationEnabled;
    private TransactionTable txTable;
    private InvocationContextContainer icc;
    private TransactionLog txLog;

    @Inject
    public void inject(Configuration configuration, TotalOrderTransactionManager totman,
                       TransactionTable txTable, InvocationContextContainer icc, TransactionLog txLog) {
        this.configuration = configuration;
        this.totman = totman;
        int numOfThreads = configuration.getNumberOfCertificationThreads();
        multithreadCertificationEnabled = numOfThreads > 1;
        certifyPool = Executors.newFixedThreadPool(numOfThreads);
        this.txTable = txTable;
        log.infof("multi thread certification scheme enabled? %s, number of threads=%s", multithreadCertificationEnabled, numOfThreads);
        this.icc = icc;
        this.txLog = txLog;
    }

    @Stop
    public void stop() {
        keysInValidation.clear();
    }

    @Override
    public Object visitTotalOrderPrepareCommand(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        Object retval = null;

        if(log.isInfoEnabled()) {
            log.infof("transaction %s is trying to commit", Util.prettyPrintGlobalTransaction(gtx));
        }

        totman.setSelfDeliverTxDuration(gtx);

        if(log.isDebugEnabled()) {
            log.debugf("transaction %s is %s,writeset is %s,is one phase commit? %s", Util.prettyPrintGlobalTransaction(gtx),
                    (ctx.isOriginLocal() ? "local" : "received in total order"),
                    command.getAffectedKeys().toString(),
                    command.isOnePhaseCommit());
        }

        if(ctx.isOriginLocal()) {
            totman.createLocalTxIfAbsent(gtx);
            retval = invokeNextInterceptor(ctx, command);
            if(command.isOnePhaseCommit()) {
                totman.removeLocalTx(gtx);
            }
        } else {
            //txLog.addGlobalTransactionInTotalOrder(command);
            totman.createRemoteTxAndSetStartTime(gtx);
            if(multithreadCertificationEnabled && certifyPool != null && !certifyPool.isTerminated()) {
                multiThreadCertification(command);
            } else {
                remoteValidation(ctx, command);
            }
        }

        return retval;
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        Object retval;
        TotalOrderTransactionManager.RemoteTxInformation remoteTx = totman.getRemoteTx(gtx);

        if(log.isInfoEnabled()) {
            log.infof("transaction %s is going to rollback", Util.prettyPrintGlobalTransaction(gtx));
        }

        if(log.isDebugEnabled()) {
            log.debugf("transaction %s is %s, needs to be send to other replicas? %s",
                    Util.prettyPrintGlobalTransaction(gtx),
                    (ctx.isOriginLocal() ? "local" : "remote"),
                    remoteTx != null && ctx.isOriginLocal());
        }

        if(ctx.isOriginLocal()) {
            long start = -1;
            if(remoteTx != null) {
                start = System.nanoTime();
                totman.incrementTransaction(false);
            } else {
                command.setNeededToBeSend(false);
            }
            retval = invokeNextInterceptor(ctx, command);
            long end = System.nanoTime();

            if(remoteTx != null) {
                remoteTx.commitOrRollbackCommandReceived();
            }

            if(start > 0) {
                totman.addLocalDuration(end - start, false);
            }
            totman.removeLocalTx(gtx);
        } else {
            try {
                if(remoteTx == null) {
                    remoteTx = totman.createRemoteTxIfAbsent(gtx);
                }

                long start = System.nanoTime();
                remoteTx.waitUntilRollbackPossible();
                long endStart = System.nanoTime();

                totman.incrementTransaction(false);

                RemoteTransaction rt = txTable.getRemoteTransaction(gtx);
                rt.invalidate();
                ((RemoteTxInvocationContext)ctx).setRemoteTransaction(rt);

                retval = invokeNextInterceptor(ctx, command);
                long end = System.nanoTime();

                totman.addRemoteDuration(end - endStart, endStart - start, false);
            } finally {
                if(remoteTx != null) {
                    remoteTx.commitOrRollbackCommandReceived();
                }
            }
        }

        if(remoteTx != null && remoteTx.canBeRemoved()) {
            totman.removeRemoteTx(gtx);
        }

        if(log.isInfoEnabled()) {
            log.infof("transaction %s finishes the rollback", Util.prettyPrintGlobalTransaction(gtx));
        }
        return retval;
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        Object retval;
        TotalOrderTransactionManager.RemoteTxInformation remoteTx = totman.createRemoteTxIfAbsent(gtx);

        if(log.isInfoEnabled()){
            log.infof("transaction %s is going to commit", Util.prettyPrintGlobalTransaction(gtx));
        }

        if(log.isDebugEnabled()) {
            log.debugf("transaction %s is %s",
                    Util.prettyPrintGlobalTransaction(gtx),
                    (ctx.isOriginLocal() ? "local" : "remote"));
        }

        totman.incrementTransaction(true);
        if(ctx.isOriginLocal()) {
            long start = System.nanoTime();
            retval = invokeNextInterceptor(ctx, command);
            remoteTx.commitOrRollbackCommandReceived();
            long end = System.nanoTime();
            totman.removeLocalTx(gtx);

            totman.addLocalDuration(end - start, true);
        } else {
            try {
                long start = System.nanoTime();
                remoteTx.waitUntilCommitPossible();
                long endStart = System.nanoTime();

                RemoteTransaction rt = txTable.getRemoteTransaction(gtx);
                ((RemoteTxInvocationContext)ctx).setRemoteTransaction(rt);

                retval = invokeNextInterceptor(ctx, command);
                long end = System.nanoTime();

                totman.addRemoteDuration(end - endStart, endStart - start, true);
            } finally {
                remoteTx.commitOrRollbackCommandReceived();
            }
        }
        if(remoteTx.canBeRemoved()) {
            totman.removeRemoteTx(gtx);
        }

        if(log.isInfoEnabled()){
            log.infof("transaction %s finishes the commit", Util.prettyPrintGlobalTransaction(gtx));
        }
        return retval;
    }

    protected Object remoteValidation(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        Object retval;
        TotalOrderTransactionManager.LocalTxInformation localTx = totman.getLocalTx(gtx);
        TotalOrderTransactionManager.RemoteTxInformation remoteTx = totman.createRemoteTxIfAbsent(gtx);
        boolean isOnePhase = command.isOnePhaseCommit();

        long end = System.nanoTime();
        long start = remoteTx.getStart();
        if(start > 0) {
            totman.addWaitingValidation(end - start);
        }

        if(log.isInfoEnabled()) {
            log.infof("validating remote transaction %s.",
                    Util.prettyPrintGlobalTransaction(gtx));
        }

        if(log.isDebugEnabled()) {
            log.debugf("write set of transaction %s is %s", Util.prettyPrintGlobalTransaction(gtx),
                    command.getAffectedKeys().toString());
        }

        start = System.nanoTime();
        try {
            if(!isOnePhase) {
                Map<Object, Object> writeSkewValues = command.getKeysAndValuesForWriteSkewCheck();
                if(!totman.checkWriteSkew(writeSkewValues)) {
                    throw new WriteSkewException("Write Skew Check fails in Total Order for keys " + writeSkewValues.keySet());
                }
            }

            retval = invokeNextInterceptor(ctx, command);
            if(log.isDebugEnabled()) {
                log.debugf("validation *successful* for transactions %s", Util.prettyPrintGlobalTransaction(gtx));
            }
            if(command.isOnePhaseCommit()) {
                totman.incrementTransaction(true);
            }
        } catch(Throwable t) {
            if(command.isOnePhaseCommit()) {
                totman.incrementTransaction(false);
            }
            retval = t;
            if(log.isDebugEnabled()) {
                log.debugf("validation *failed* for transactions %s because of %s",
                        Util.prettyPrintGlobalTransaction(gtx),
                        t.getLocalizedMessage());
            }
        }
        end = System.nanoTime();
        totman.addValidationDuration(end - start, command.isOnePhaseCommit());

        if(localTx != null) {
            localTx.setRetValue(retval);
            localTx.markValidationCompelete();
        }

        remoteTx.validationCompeted();

        transactionsFinished(remoteTx, localTx, retval, command);

        //ie is 2PC
        if(!isOnePhase) {
            if(log.isInfoEnabled()) {
                log.infof("transaction %s needs 2 Phases to commit... waiting for the second phase",
                        Util.prettyPrintGlobalTransaction(gtx));
            }
            start = System.nanoTime();
            remoteTx.waitUntilCommitOrRollback();
            end = System.nanoTime();
            if(log.isInfoEnabled()) {
                log.infof("transaction %s finishes waiting for the second phase. returning...",
                        Util.prettyPrintGlobalTransaction(gtx));
            }
            totman.add2PhaseWaitingDuration(end - start);
        } else {
            remoteTx.commitOrRollbackCommandReceived();
        }

        if(remoteTx.canBeRemoved()) {
            totman.removeRemoteTx(gtx);
        }
        return retval;
    }

    protected void transactionsFinished(TotalOrderTransactionManager.RemoteTxInformation remoteTx,
                                        TotalOrderTransactionManager.LocalTxInformation localTx,
                                        Object retval, TotalOrderPrepareCommand command) throws InterruptedException {
        //we don't need a vote phase
        if(localTx != null) {
            localTx.markAllKeysValidatedAndReturnThisObject(retval);
        }
    }

    protected Set<Object> getAffectedKeys(TotalOrderPrepareCommand command) {
        return command.getAffectedKeys(); //in full replication is all set...
    }

    protected void multiThreadCertification(TotalOrderPrepareCommand command) {
        Set<Object> keys = getAffectedKeys(command);
        CertifyThread ct = new CertifyThread(keys, command, (TxInvocationContext) icc.suspend());
        CountDownLatch cdl = ct.get();
        for(Object key : keys) {
            CountDownLatch owner = keysInValidation.put(key, cdl);
            if(owner != null) {
                ct.addPrecedingTx(owner);
            }
        }

        if(trace) {
            log.tracef("multi-threading validation. transaction is %s, keys to be check is %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    keys.toString());
        }

        certifyPool.execute(ct);
    }

    private class CertifyThread implements Runnable {

        private Set<CountDownLatch> precedingTxs;
        private Set<Object> keys;
        private CountDownLatch barrier;
        private TotalOrderPrepareCommand command;
        private TxInvocationContext ctx;

        public CertifyThread(Collection<Object> keys, TotalOrderPrepareCommand command, TxInvocationContext ctx){
            this.precedingTxs = new HashSet<CountDownLatch>();
            this.keys = new HashSet<Object>(keys);
            barrier = new CountDownLatch(1);
            this.command = command;
            this.ctx = ctx;
        }

        public CertifyThread(TotalOrderPrepareCommand command, TxInvocationContext ctx) {
            this.precedingTxs = Collections.emptySet();
            this.keys = Collections.emptySet();
            barrier = new CountDownLatch(1);
            this.command = command;
            this.ctx = ctx;
        }

        public void addPrecedingTx(CountDownLatch cdl) {
            if(cdl == barrier) {
                return; //avoid deadlock
            }
            precedingTxs.add(cdl);
        }

        public CountDownLatch get() {
            return barrier;
        }

        @Override
        public void run() {
            GlobalTransaction gtx = command.getGlobalTransaction();
            try {
                icc.resume(ctx);
                if(trace) {
                    log.tracef("transaction %s is going to wait for preceding transactions to finish",
                            Util.prettyPrintGlobalTransaction(gtx));
                }

                for(CountDownLatch cdl : precedingTxs) {
                    cdl.await();
                }

                remoteValidation(ctx, command);
            } catch (InterruptedException e) {
                log.warnf("Interrupted exception caughted while validating transaction %s. Ignoring...",
                        Util.prettyPrintGlobalTransaction(gtx));
            } catch (Throwable throwable) {
                if(log.isDebugEnabled()) {
                    log.debugf("Ignoring exception caughted while validation transaction %s. Exception is %s and the message is %s",
                            Util.prettyPrintGlobalTransaction(gtx),
                            throwable.getClass().toString(),
                            throwable.getLocalizedMessage());
                    throwable.printStackTrace();
                }
            } finally {
                if(trace) {
                    log.tracef("transaction %s is going to releasing blocked keys",
                            Util.prettyPrintGlobalTransaction(gtx));
                }
                barrier.countDown();
                for(Object key : keys) {
                    keysInValidation.remove(key, barrier);
                }
            }
        }

    }
}
