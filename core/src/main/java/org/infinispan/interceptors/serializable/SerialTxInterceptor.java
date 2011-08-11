package org.infinispan.interceptors.serializable;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.container.DataContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.interceptors.TxInterceptor;

import java.util.Set;

/**
 * @author pruivo
 *         Date: 11/08/11
 */
public class SerialTxInterceptor extends TxInterceptor {
    private CommandsFactory commandsFactory;
    @Inject
    public void inject(CommandsFactory commandsFactory) {
        this.commandsFactory = commandsFactory;
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        Set<Object> writeSet = command.getAffectedKeys();
        Set<Object> readSet = command.getReadSet();

        //first acquire the read write locks and (second) validate the readset
        AcquireValidationLocksCommand  locksCommand = commandsFactory.buildAcquireValidationLocksCommand(
                readSet, writeSet, command.getVersion());
        invokeNextInterceptor(ctx, locksCommand);

        //third (this is equals to old schemes) wrap the wrote entries
        //finally, process the rest of the command
        return super.visitPrepareCommand(ctx, command);
    }
}
