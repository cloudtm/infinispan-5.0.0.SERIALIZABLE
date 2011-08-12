package org.infinispan.commands.tx;

import org.infinispan.commands.LocalCommand;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Collections;
import java.util.Set;

/**
 * @author pruivo
 *         Date: 11/08/11
 */
public class AcquireValidationLocksCommand implements VisitableCommand, LocalCommand{

    private Set<Object> writeSet;
    private Set<Object> readSet;
    private VersionVC version;
    private GlobalTransaction gtx;

    public AcquireValidationLocksCommand(GlobalTransaction gtx, Set<Object> readSet, Set<Object> writeSet, VersionVC version) {
        this.writeSet = writeSet;
        this.readSet = readSet;
        this.version = version;
        this.gtx = gtx;
    }

    public Set<Object> getWriteSet() {
        return Collections.unmodifiableSet(writeSet);
    }

    public Set<Object> getReadSet() {
        return Collections.unmodifiableSet(readSet);
    }

    public VersionVC getVersion() {
        return version;
    }

    public GlobalTransaction getGlobalTransaction() {
        return gtx;
    }

    @Override
    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return visitor.visitAcquireValidationLocksCommand((TxInvocationContext) ctx, this);
    }

    @Override
    public boolean shouldInvoke(InvocationContext ctx) {
        return false;
    }

    @Override
    public Object perform(InvocationContext ctx) throws Throwable {
        return null;
    }

    @Override
    public byte getCommandId() {
        return 0;
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public void setParameters(int commandId, Object[] parameters) {
        //no-op
    }
}
