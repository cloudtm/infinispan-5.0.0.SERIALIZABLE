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

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.RemoteTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.transaction.RemoteTransaction;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.transaction.xa.recovery.RecoveryManager;
import org.infinispan.util.Util;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import sun.tools.tree.ThisExpression;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Command corresponding to the 1st phase of 2PC.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class PrepareCommand extends AbstractTransactionBoundaryCommand {

    private static final Log log = LogFactory.getLog(PrepareCommand.class);
    private boolean trace = log.isTraceEnabled();

    public static final byte COMMAND_ID = 12;

    protected WriteCommand[] modifications;
    protected boolean onePhaseCommit;
    protected CacheNotifier notifier;
    protected RecoveryManager recoveryManager;
    protected Object[] readSet;
    protected VersionVC version;
    //We don't want to serialize this factory
    protected transient VersionVCFactory versionVCFactory;


    public void initialize(CacheNotifier notifier, RecoveryManager recoveryManager, VersionVCFactory versionVCFactory) {
        this.notifier = notifier;
        this.recoveryManager = recoveryManager;
        this.versionVCFactory=versionVCFactory;
    }

    public PrepareCommand(GlobalTransaction gtx, boolean onePhaseCommit, Object[] readSet, VersionVC version,  List<WriteCommand> commands) {
        this.globalTx = gtx;
        this.modifications = commands == null || commands.isEmpty() ? null : commands.toArray(new WriteCommand[commands.size()]);
        this.onePhaseCommit = onePhaseCommit;
        this.readSet = readSet;
        this.version = version;
    }

    public PrepareCommand(GlobalTransaction gtx, boolean onePhaseCommit, WriteCommand... modifications) {
        this.globalTx = gtx;
        this.modifications = modifications;
        this.onePhaseCommit = onePhaseCommit;
        this.readSet = null;
        this.version = null;
    }

    public PrepareCommand(GlobalTransaction gtx, List<WriteCommand> commands, boolean onePhaseCommit) {
        this.globalTx = gtx;
        this.modifications = commands == null || commands.isEmpty() ? null : commands.toArray(new WriteCommand[commands.size()]);
        this.onePhaseCommit = onePhaseCommit;
        this.readSet = null;
        this.version = null;
    }

    public PrepareCommand() {
    }

    public Object perform(InvocationContext ignored) throws Throwable {

        //log.warnf("Received Prepare for Transaction %s", Util.prettyPrintGlobalTransaction(globalTx));

        if (ignored != null)
            throw new IllegalStateException("Expected null context!");

        if (recoveryManager != null && recoveryManager.isTransactionPrepared(globalTx)) {
            log.tracef("The transaction %s is already prepared. Skipping prepare call.", globalTx);
            return null;
        }

        //log.warnf("Received Prepare for Transaction %s", Util.prettyPrintGlobalTransaction(globalTx));

        RemoteTransaction remoteTransaction = null;
        try{
        // 1. first create a remote transaction
        remoteTransaction = txTable.getRemoteTransaction(globalTx);
        boolean remoteTxInitiated = remoteTransaction != null;

        boolean removeOnOutOfOrderRollback = false;

        if (!remoteTxInitiated) {
            remoteTransaction = txTable.createRemoteTransaction(globalTx, modifications);

            removeOnOutOfOrderRollback = true;

        } else {
            /*
            * remote tx was already created by Cache#lock() API call
            * set the proper modifications since lock has none
            *
            * @see LockControlCommand.java
            * https://jira.jboss.org/jira/browse/ISPN-48
            */
            remoteTransaction.setModifications(getModifications());
        }




        //Now I check if a rollback for this transaction is already arrived. That was an out of order rollback.
        //This happens when fifo is not guaranteed (because we can use JGroups OOB threads) and the coordinator for this transaction
        //sends the rollback to this node before waiting for the ack for a prepare on this node (e.g. replication timeout exception).

        boolean toBeRemoved = txTable.isOutOfOrderRollback(globalTx);

        if(toBeRemoved){//The rollback has already touched this node because an out of order rollback was registered in the txTable

            log.warnf("Rollback is already arrived for transaction %s", Util.prettyPrintGlobalTransaction(globalTx));

            txTable.removeOutOfOrderRollback(globalTx); //We want to be sure that this entry is removed.

            if(removeOnOutOfOrderRollback){//This command had created the remote transaction.
               txTable.removeRemoteTransaction(globalTx); //Garbage collects this remote transaction because the rollback is already arrived.
            }

            return null; //The coordinator doesn't really wait for this response because it has already sent the rollback.

        }


        // 2. then set it on the invocation context
        RemoteTxInvocationContext ctx = icc.createRemoteTxInvocationContext(getOrigin());
        ctx.setRemoteTransaction(remoteTransaction);

        if (trace)
            log.tracef("Invoking remotely originated prepare: %s with invocation context: %s", this, ctx);
        notifier.notifyTransactionRegistered(ctx.getGlobalTransaction(), ctx);
        return invoker.invoke(ctx, this);
        }
        finally{
            if(remoteTransaction != null){
                //Suppose a rollback concurrently arrives. It can execute only after the completion of this prepare.
                remoteTransaction.notifyEndPrepare();
            }
        }
    }

    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return visitor.visitPrepareCommand((TxInvocationContext) ctx, this);
    }

    public WriteCommand[] getModifications() {
        return modifications == null ? new WriteCommand[]{} : modifications;
    }

    public boolean isOnePhaseCommit() {
        return onePhaseCommit;
    }

    public boolean existModifications() {
        return modifications != null && modifications.length > 0;
    }

    public int getModificationsCount() {
        return modifications != null ? modifications.length : 0;
    }

    public byte getCommandId() {
        return COMMAND_ID;
    }

    @Override
    public Object[] getParameters() {
        int numMods = modifications == null ? 0 : modifications.length;
        int numReads = readSet == null ? 0 : readSet.length;
        Object[] retval = new Object[numMods + numReads + 6];
        retval[0] = globalTx;
        retval[1] = cacheName;
        retval[2] = onePhaseCommit;
        retval[3] = version;
        retval[4] = numMods;
        retval[5] = numReads;
        if (numMods > 0) System.arraycopy(modifications, 0, retval, 6, numMods);
        if (numReads > 0) System.arraycopy(readSet, 0, retval, 6 + numMods, numReads);
        return retval;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setParameters(int commandId, Object[] args) {
        globalTx = (GlobalTransaction) args[0];
        cacheName = (String) args[1];
        onePhaseCommit = (Boolean) args[2];
        version = (VersionVC) args[3];
        int numMods = (Integer) args[4];
        int numReads = (Integer) args[5];
        if (numMods > 0) {
            modifications = new WriteCommand[numMods];
            System.arraycopy(args, 6, modifications, 0, numMods);
        }
        if(numReads > 0){
        	readSet = new Object[numReads];
        	System.arraycopy(args, 6 + numMods, readSet, 0, numReads);
        }	
    }

    public PrepareCommand copy() {
        PrepareCommand copy = new PrepareCommand();
        copy.globalTx = globalTx;
        copy.modifications = modifications == null ? null : modifications.clone();
        copy.onePhaseCommit = onePhaseCommit;
        if(readSet != null){
        	copy.readSet = new Object[readSet.length];
        	System.arraycopy(readSet, 0, copy.readSet, 0, readSet.length);
        }
        else{
        	copy.readSet = null;
        }
        
        try {
			copy.version = version != null ? version.clone() : null;
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			copy.version=null;
		}
        return copy;
    }

    @Override
    public String toString() {
        return "PrepareCommand {" +
                "gtx=" + globalTx +
                ", modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
                ", onePhaseCommit=" + onePhaseCommit +
                ", readSet=" + readSet +
                ", " + super.toString();
    }

    public boolean containsModificationType(Class<? extends ReplicableCommand> replicableCommandClass) {
        for (WriteCommand mod : getModifications()) {
            if (mod.getClass().equals(replicableCommandClass)) {
                return true;
            }
        }
        return false;
    }

    public boolean hasModifications() {
        return modifications != null && modifications.length > 0;
    }

    public Set<Object> getAffectedKeys() {
        if (modifications == null || modifications.length == 0) return Collections.emptySet();
        if (modifications.length == 1) return modifications[0].getAffectedKeys();
        Set<Object> keys = new HashSet<Object>();
        for (WriteCommand wc: modifications) keys.addAll(wc.getAffectedKeys());
        return keys;
    }

    public Object[] getReadSet() {
    	if(readSet == null) return null;
    	
    	Object[] result = new Object[readSet.length];
    	
        System.arraycopy(readSet, 0, result, 0, readSet.length);
        
        return result;
    }
    
   

    public VersionVC getVersion() {
        return version != null ? version : this.versionVCFactory.createVersionVC();
    }
}
