package org.infinispan.interceptors.serializable;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.TxInterceptor;
import org.infinispan.mvcc.CommitLog;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.Util;

import java.util.Set;

/**
 * @author pruivo
 *         Date: 11/08/11
 */
public class SerialTxInterceptor extends TxInterceptor {
    private CommandsFactory commandsFactory;
    private CommitLog commitLog;

    @Inject
    public void inject(CommandsFactory commandsFactory, CommitLog commitLog) {
        this.commandsFactory = commandsFactory;
        this.commitLog = commitLog;
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
        super.visitPrepareCommand(ctx, command);

        //create commit version (not this is full replication or local mode)
        commitLog.commitLock.lock();
        long commitVersion = commitLog.getPrepareCounter();
        VersionVC commitVC = new VersionVC();
        commitVC.set(0, commitVersion);
        ctx.setCommitVersion(commitVC);

        log.debugf("transaction [%s] commit vector clock is %s",
                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                commitVC);
        return null;
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        Object retval = super.visitRollbackCommand(ctx, command);
        commitLog.commitLock.unlock();
        return retval;
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        Object retval = super.visitGetKeyValueCommand(ctx, command);
        if(ctx.isInTxScope()) {
            ((TxInvocationContext) ctx).markReadFrom(0);
        }
        return retval;
    }
}
