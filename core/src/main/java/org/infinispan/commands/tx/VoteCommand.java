package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.context.InvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.Util;

import java.util.HashSet;
import java.util.Set;

/**
 * @author pedro
 *         Date: 21-06-2011
 */
public class VoteCommand extends AbstractTransactionBoundaryCommand {
    public static final byte COMMAND_ID = 26;

    private boolean success;
    private Set<Object> keysValidated;

    public VoteCommand(GlobalTransaction gtx, boolean success, Set<Object> keysValidated) {
        this.globalTx = gtx;
        this.success = success;
        this.keysValidated = new HashSet<Object>(keysValidated);
    }

    public VoteCommand() {
        keysValidated = new HashSet<Object>();
    }

    @Override
    public Object perform(InvocationContext ctx) throws Throwable {
        globalTx.setRemote(true);
        return invoker.invoke(icc.createRemoteTxInvocationContext(getOrigin()), this);
    }

    @Override
    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return null;
    }

    @Override
    public byte getCommandId() {
        return COMMAND_ID;
    }

    public boolean isVoteOK() {
        return success;
    }

    public Set<Object> getValidatedKeys() {
        return keysValidated;
    }

    @Override
    public Object[] getParameters() {
        if(success) {
            return new Object[] {globalTx, success, keysValidated};
        } else {
            return new Object[] {globalTx, success};
        }
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public void setParameters(int commandId, Object[] args) {
        globalTx = (GlobalTransaction) args[0];
        success = (Boolean) args[1];
        if(success) {
            keysValidated = (Set<Object>) args[2];
        }
    }

    @Override
    public String toString() {
        return "{VoteCommand: tx:" + Util.prettyPrintGlobalTransaction(globalTx) +
                ",result:" + success +
                ",keys validated:" + keysValidated +
                "}";
    }
}

