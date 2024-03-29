/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Set;

/**
 * Command corresponding to the 2nd phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class CommitCommand extends AbstractTransactionBoundaryCommand {
    public static final byte COMMAND_ID = 14;
    /**
     * This is sent back to callers if the global transaction is not reconised.  It can happen if a prepare is sent to one
     * set of nodes, which then fail, and a commit is sent to a "new" data owner which has not seen the prepare.
     *
     * Responding with this value instructs the caller to re-send the prepare.  See DistributionInterceptor.visitCommitCommand()
     * for details.
     */
    public static final byte RESEND_PREPARE = 1;

    private VersionVC commitVersion;

    public CommitCommand(GlobalTransaction gtx) {
        this.globalTx = gtx;
    }

    public CommitCommand(GlobalTransaction gtx, VersionVC commitVersion) {
        this.globalTx = gtx;
        this.commitVersion = commitVersion;
    }

    public CommitCommand() {
    }

    @Override
    protected Object invalidRemoteTxReturnValue() {
        return RESEND_PREPARE;
    }

    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return visitor.visitCommitCommand((TxInvocationContext) ctx, this);
    }

    public byte getCommandId() {
        return COMMAND_ID;
    }

    @Override
    public String toString() {
        return "CommitCommand {" + super.toString();
    }

    @Override
    public Object perform(InvocationContext ctx) throws Throwable {
        if(configuration.isTotalOrderReplication()) {
            return totalOrderPerform(ctx);
        } else {
            return super.perform(ctx);
        }
    }

    private Object totalOrderPerform(InvocationContext ctx) {
        if (ctx != null) throw new IllegalStateException("Expected null context!");
        globalTx.setRemote(true);
        RemoteTxInvocationContext ctxt = icc.createRemoteTxInvocationContext(getOrigin());
        return invoker.invoke(ctxt, this);
    }

    public VersionVC getCommitVersion() {
        return commitVersion;
    }

    @Override
    public Object[] getParameters() {
        Object[] retval = new Object[3];
        retval[0] = globalTx;
        retval[1] = cacheName;
        retval[2] = commitVersion;
        return retval;
    }

    @Override
    public void setParameters(int commandId, Object[] args) {
        globalTx = (GlobalTransaction) args[0];
        cacheName = (String) args[1];
        commitVersion = (VersionVC) args[2];
    }
}
