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
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Command corresponding to a transaction rollback.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class RollbackCommand extends AbstractTransactionBoundaryCommand {
    public static final byte COMMAND_ID = 13;

    private static final Log log = LogFactory.getLog(RollbackCommand.class);

    private boolean neededToBeSend = true;

    public RollbackCommand(GlobalTransaction globalTransaction) {
        this.globalTx = globalTransaction;
    }

    public RollbackCommand() {
    }

    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return visitor.visitRollbackCommand((TxInvocationContext) ctx, this);
    }

    @Override
    public void visitRemoteTransaction(RemoteTransaction tx) {
        tx.invalidate();
    }

    public byte getCommandId() {
        return COMMAND_ID;
    }

    @Override
    public String toString() {
        return "RollbackCommand {" + super.toString();
    }

    public boolean isNeededToBeSend() {
        return neededToBeSend;
    }

    public void setNeededToBeSend(boolean neededToBeSend) {
        this.neededToBeSend = neededToBeSend;
    }

    @Override
    public Object perform(InvocationContext ctx) throws Throwable {
        if(configuration.isTotalOrderReplication()) {
            return totalOrderPerform(ctx);
        } else {


            if (ctx != null) throw new IllegalStateException("Expected null context!");
            this.globalTx.setRemote(true);

            RemoteTransaction transaction = txTable.getRemoteTransaction(globalTx);

            boolean garbageCollectOutOfOrderRollback = false;

            if (transaction == null) { //This transaction is not prepared here

              txTable.setOutOfOrderRollback(globalTx);
              log.warnf("Registered out of order Rollback for transaction %s", Util.prettyPrintGlobalTransaction(globalTx));


              //Now I've marked that rollback is arrived before the prepare.



              //Is the prepare arrived now?
              transaction = txTable.getRemoteTransaction(globalTx);

              if(transaction != null){//Yes

                 garbageCollectOutOfOrderRollback = true;


              }


            }

            if(transaction == null){

               log.warnf("Prepare not arrived for transaction %s", Util.prettyPrintGlobalTransaction(globalTx));
               //This means that:
               // - the prepare is not arrived yet or
               // - the prepare has seen the "out of order rollback" mark and it has not been performed.

               //In both cases return null. We don't need to rollback a not prepared transaction.

              return invalidRemoteTxReturnValue();
            }

            //Transaction is not null. The prepare has been performed or it is still running.

            if(garbageCollectOutOfOrderRollback){//We can garbage collects out of order information.

                log.warnf("Registered out of order Rollback is going to be removed for transaction %s", Util.prettyPrintGlobalTransaction(globalTx));

                txTable.removeOutOfOrderRollback(globalTx);
            }

            //If the prepare is running I should wait for its completion.

            transaction.waitForPrepare();

            visitRemoteTransaction(transaction);
            RemoteTxInvocationContext ctxt = icc.createRemoteTxInvocationContext(getOrigin());
            ctxt.setRemoteTransaction(transaction);


            return invoker.invoke(ctxt, this);


        }
    }

    private Object totalOrderPerform(InvocationContext ctx) {
        if (ctx != null) throw new IllegalStateException("Expected null context!");
        globalTx.setRemote(true);
        RemoteTxInvocationContext ctxt = icc.createRemoteTxInvocationContext(getOrigin());
        return invoker.invoke(ctxt, this);
    }
}
