package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.Util;

import java.util.Set;

/**
 * @author pruivo
 *         Date: 11/08/11
 */
public class SerialTxInterceptor extends TxInterceptor {
    private CommandsFactory commandsFactory;
    private CommitQueue commitQueue;
    private InvocationContextContainer icc;

    @Inject
    public void inject(CommandsFactory commandsFactory, CommitQueue commitQueue, InvocationContextContainer icc) {
        this.commandsFactory = commandsFactory;
        this.commitQueue = commitQueue;
        this.icc = icc;
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        Set<Object> writeSet = command.getAffectedKeys();
        Set<Object> readSet = command.getReadSet();

        if(writeSet != null && writeSet.isEmpty()) {
            log.debugf("new transaction [%s] arrived to SerialTxInterceptor. it is a Read-Only Transaction. returning",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            return null;
        }

        log.debugf("new transaction [%s] arrived to SerialTxInterceptor. readSet=%s, writeSet=%s, version=%s",
                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                readSet, writeSet, command.getVersion());

        //first acquire the read write locks and (second) validate the readset
        AcquireValidationLocksCommand  locksCommand = commandsFactory.buildAcquireValidationLocksCommand(
                command.getGlobalTransaction(), readSet, writeSet, command.getVersion());
        invokeNextInterceptor(ctx, locksCommand);

        log.debugf("transaction [%s] passes validation",
                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));

        //third (this is equals to old schemes) wrap the wrote entries
        //finally, process the rest of the command
        Object retVal = super.visitPrepareCommand(ctx, command);

        VersionVC commitVC = commitQueue.addTransaction(command.getGlobalTransaction(), ctx.calculateVersionToRead(),
                icc.getInvocationContext().clone(), 0);

        if(retVal != null && retVal instanceof VersionVC) {
            VersionVC othersCommitVC = (VersionVC) retVal;
            commitVC.setToMaximum(othersCommitVC);
        }
        ctx.setCommitVersion(commitVC);

        log.debugf("transaction [%s] commit vector clock is %s",
                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                commitVC);
        return commitVC;
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        log.debugf("received commit command for %s", Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        if(ctx.isInTxScope() && ctx.isOriginLocal() && !ctx.hasModifications()) {
            log.debugf("try commit a read-only transaction [%s]. returning...",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            return null;
        }

        /*log.warnf("looked up keys for %s are %s",
                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                ctx.getLookedUpEntries());*/
        return super.visitCommitCommand(ctx, command);
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        log.debugf("received rollback command for %s", Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        return super.visitRollbackCommand(ctx, command);
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        Object retval = super.visitGetKeyValueCommand(ctx, command);
        if(ctx.isInTxScope()) {
            ((TxInvocationContext) ctx).markReadFrom(0);
            //update vc
            InternalMVCCEntry ime = ((TxInvocationContext) ctx).getReadKey(command.getKey());
            if(ime == null) {
                log.warn("InternalMVCCEntry is null.");
            } else if(((TxInvocationContext) ctx).hasModifications() && !ime.isMostRecent()) {
                //read an old value... the tx will abort in commit,
                //so, do not waste time and abort it now
                throw new CacheException("transaction must abort!! read an old value and it is not a read only transaction");
            } else {
                ((TxInvocationContext) ctx).updateVectorClock(ime.getVersion());
            }
        }

        return retval;
    }
}
