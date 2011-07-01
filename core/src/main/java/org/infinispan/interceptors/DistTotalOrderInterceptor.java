package org.infinispan.interceptors;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.commands.tx.VoteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.totalorder.TotalOrderTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author pedro
 *         Date: 16-06-2011
 */
public class DistTotalOrderInterceptor extends TotalOrderInterceptor {
    private RpcManager rpcManager;
    private boolean votesNeeded;
    private CommandsFactory commandsFactory;
    private DistributionManager distManager;

    @Inject
    public void init(Configuration configuration, RpcManager rpcManager, CommandsFactory commandsFactory,
                     DistributionManager distManager) {
        this.configuration = configuration;
        this.rpcManager = rpcManager;
        this.commandsFactory = commandsFactory;
        this.distManager = distManager;

        votesNeeded = configuration.isTotalOrderPartialReplication() &&
                configuration.isWriteSkewCheck() &&
                configuration.getIsolationLevel() == IsolationLevel.REPEATABLE_READ;
    }

    @Override
    public Object visitVoteCommand(TxInvocationContext ctx, VoteCommand command) throws Throwable {
        if(log.isDebugEnabled()) {
            log.debugf("vote received for transactions %s. vote is %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    command.toString());
        }
        totman.addVote(command.getGlobalTransaction(), command.isVoteOK(), command.getValidatedKeys());
        return null;
    }

    @Override
    protected void transactionsFinished(TotalOrderTransactionManager.RemoteTxInformation remoteTx, TotalOrderTransactionManager.LocalTxInformation localTx, Object retval, TotalOrderPrepareCommand command) throws InterruptedException {
        boolean isOnePhaseCommit = command.isOnePhaseCommit();

        //ie is 2PC
        if(!isOnePhaseCommit) {
            GlobalTransaction gtx = command.getGlobalTransaction();
            Set<Object> writeSkewKeys = command.getKeysAndValuesForWriteSkewCheck().keySet();
            Set<Object> keysValidated = getOnlyLocalKeys(writeSkewKeys);
            if(votesNeeded && !keysValidated.isEmpty()) {
                boolean txResult = !(retval instanceof Exception);
                if(localTx == null) {
                    //the tx is not local
                    VoteCommand vote = commandsFactory.buildVoteCommand(gtx, txResult, keysValidated);
                    if(log.isDebugEnabled()) {
                        log.debugf("sending this %s to %s", vote.toString(), gtx.getAddress().toString());
                    }
                    rpcManager.invokeRemotely(Collections.singleton(gtx.getAddress()), vote, false);
                } else {
                    //the tx is local
                    if(log.isDebugEnabled()) {
                        log.debugf("Transaction %s is local. add vote. result=%s, key validated=%s",
                                Util.prettyPrintGlobalTransaction(gtx),
                                txResult,
                                keysValidated);
                    }
                    totman.addVote(gtx, txResult, keysValidated);
                }
            }
        }
    }

    protected Set<Object> getAffectedKeys(TotalOrderPrepareCommand command) {
        return getOnlyLocalKeys(command.getAffectedKeys());
    }

    private Set<Object> getOnlyLocalKeys(Set<Object> all) {
        Set<Object> local = new HashSet<Object>();
        for(Object key : all) {
            if(distManager.getLocality(key).isLocal()) {
                local.add(key);
            }
        }
        return local;
    }
}
