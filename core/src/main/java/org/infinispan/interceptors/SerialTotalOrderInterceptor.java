package org.infinispan.interceptors;

import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.totalorder.TotalOrderTransactionManager;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.TransactionTable;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;

/**
 * @author pedro
 *         Date: 07-10-2011
 */
//keeps track of the transaction, order the commit/rollback with the prepare
public class SerialTotalOrderInterceptor extends CommandInterceptor {
    protected TotalOrderTransactionManager totman;
    private TransactionTable txTable;

    private boolean debug;

    @Inject
    public void inject(TotalOrderTransactionManager totman, TransactionTable txTable) {
        this.totman = totman;
        this.txTable = txTable;
    }

    @Start
    public void setLogLevel() {
        debug = log.isDebugEnabled();
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        Object retval;
        TotalOrderTransactionManager.RemoteTxInformation remoteTx = totman.getRemoteTx(gtx);

        if(debug) {
            log.debugf("Receive a Rollback Command for transaction %s (%s), does it need to send to other replicas? %s",
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

            command.setNeededToBeSend(true); //send it anyway
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

        if(debug) {
            log.debugf("Transaction %s finishes the processing of Rollback Command",
                    Util.prettyPrintGlobalTransaction(gtx));
        }
        return retval;
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        Object retval;
        TotalOrderTransactionManager.RemoteTxInformation remoteTx = totman.createRemoteTxIfAbsent(gtx);

        if(debug){
            log.debugf("Commit Command received for transaction %s (%s)", Util.prettyPrintGlobalTransaction(gtx),
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

        if(debug){
            log.debugf("Transaction %s finishes the processing of Commit Command",
                    Util.prettyPrintGlobalTransaction(gtx));
        }
        return retval;
    }

    @Override
    public Object visitTotalOrderPrepareCommand(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        GlobalTransaction gtx = command.getGlobalTransaction();
        Object retval = null;

        totman.setSelfDeliverTxDuration(gtx);

        

        if(ctx.isOriginLocal()) {
            totman.createLocalTxIfAbsent(gtx);
            retval = invokeNextInterceptor(ctx, command);
            if(command.isOnePhaseCommit()) {
                totman.removeLocalTx(gtx);
            }
        } else {
            TotalOrderTransactionManager.RemoteTxInformation remoteTx = totman.createRemoteTxAndSetStartTime(gtx);
            try {
                retval = invokeNextInterceptor(ctx, command);
            } finally {
                remoteTx.validationCompeted();
                if(remoteTx.canBeRemoved()) {
                    totman.removeRemoteTx(gtx);
                }
            }
        }

        return retval;
    }
}
