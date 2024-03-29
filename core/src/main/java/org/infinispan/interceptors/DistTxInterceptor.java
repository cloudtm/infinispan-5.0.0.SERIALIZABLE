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
package org.infinispan.interceptors;


import org.infinispan.commands.AbstractVisitor;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.commands.write.ClearCommand;
import org.infinispan.commands.write.DataWriteCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.PutMapCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.rhq.helpers.pluginAnnotations.agent.DisplayType;
import org.rhq.helpers.pluginAnnotations.agent.MeasurementType;
import org.rhq.helpers.pluginAnnotations.agent.Metric;
import org.rhq.helpers.pluginAnnotations.agent.Operation;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A special form of the TxInterceptor that is aware of distribution and consistent hashing, and as such only replays
 * methods during a remote prepare that are targeted to this specific cache instance.
 *
 * @author Manik Surtani
 * @since 4.0
 */
@MBean(objectName = "Transactions", description = "Component that manages the cache's participation in JTA transactions.")
public class DistTxInterceptor extends TxInterceptor {

   DistributionManager dm;
   ReplayCommandVisitor replayCommandVisitor = new ReplayCommandVisitor();
   private CommandsFactory commandsFactory;
   
   
   private final AtomicLong nrLocalReadOp = new AtomicLong(0);
   private final AtomicLong localReadTime = new AtomicLong(0);

   @Inject
   public void injectDistributionManager(DistributionManager dm, CommandsFactory commandsFactory) {
      this.dm = dm;
      this.commandsFactory = commandsFactory;
   }

   /**
    * Only replays modifications that are
    */
   @Override
   protected VisitableCommand getCommandToReplay(VisitableCommand command) {
      try {
         return (VisitableCommand) command.acceptVisitor(null, replayCommandVisitor);
      } catch (RuntimeException re) {
         throw re;
      } catch (Throwable th) {
         throw new RuntimeException(th);
      }
   }

   @Override
   public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand cmd) throws Throwable {
      dm.getTransactionLogger().beforeCommand(cmd);
      try {
         return super.visitPrepareCommand(ctx, cmd);
      } finally {
         dm.getTransactionLogger().afterCommand(cmd);
      }
   }

   @Override
   public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand cmd) throws Throwable {
      dm.getTransactionLogger().beforeCommand(cmd);
      try {
         return super.visitRollbackCommand(ctx, cmd);
      } finally {
         dm.getTransactionLogger().afterCommand(cmd);
      }
   }

   @Override
   public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand cmd) throws Throwable {
      dm.getTransactionLogger().beforeCommand(cmd, ctx);
      try {
         return super.visitCommitCommand(ctx, cmd);
      } finally {
         dm.getTransactionLogger().afterCommand(cmd, ctx);
      }
   }

   @Override
   public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
      if (!ctx.isInTxScope()) dm.getTransactionLogger().beforeCommand(command);
      try {
         return super.visitPutKeyValueCommand(ctx, command);
      } finally {
         if (!ctx.isInTxScope()) dm.getTransactionLogger().afterCommand(command);
      }
   }

   @Override
   public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
      if (!ctx.isInTxScope()) dm.getTransactionLogger().beforeCommand(command);
      try {
         return super.visitRemoveCommand(ctx, command);
      } finally {
         if (!ctx.isInTxScope()) dm.getTransactionLogger().afterCommand(command);
      }
   }

   @Override
   public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
      if (!ctx.isInTxScope()) dm.getTransactionLogger().beforeCommand(command);
      try {
         return super.visitReplaceCommand(ctx, command);
      } finally {
         if (!ctx.isInTxScope()) dm.getTransactionLogger().afterCommand(command);
      }
   }

   @Override
   public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
      if (!ctx.isInTxScope()) dm.getTransactionLogger().beforeCommand(command);
      try {
         return super.visitClearCommand(ctx, command);
      } finally {
         if (!ctx.isInTxScope()) dm.getTransactionLogger().afterCommand(command);
      }
   }

   @Override
   public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
      if (!ctx.isInTxScope()) dm.getTransactionLogger().beforeCommand(command);
      try {
         return super.visitPutMapCommand(ctx, command);
      } finally {
         if (!ctx.isInTxScope()) dm.getTransactionLogger().afterCommand(command);
      }
   }


   class ReplayCommandVisitor extends AbstractVisitor {
      @Override
      public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) {
         Map newMap = new HashMap();
         for (Map.Entry entry : command.getMap().entrySet()) {
            if (dm.getLocality(entry.getKey()).isLocal()) newMap.put(entry.getKey(), entry.getValue());
         }

         if (newMap.isEmpty()) return null;
         if (newMap.size() == command.getMap().size()) return command;
         return commandsFactory.buildPutMapCommand(newMap, command.getLifespanMillis(), command.getMaxIdleTimeMillis(), ctx.getFlags());
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ignored, PutKeyValueCommand command) {
         return visitDataWriteCommand(command);
      }

      @Override
      public Object visitRemoveCommand(InvocationContext ignored, RemoveCommand command) {
         return visitDataWriteCommand(command);
      }

      @Override
      public Object visitReplaceCommand(InvocationContext ignored, ReplaceCommand command) {
         return visitDataWriteCommand(command);
      }

      private VisitableCommand visitDataWriteCommand(DataWriteCommand command) {
         return dm.getLocality(command.getKey()).isLocal() ? command : null;
      }

      @Override
      public Object handleDefault(InvocationContext ignored, VisitableCommand command) {
         return command;
      }
   }

    @Override
    public Object visitTotalOrderPrepareCommand(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        dm.getTransactionLogger().beforeCommand(command);
      try {
         return super.visitTotalOrderPrepareCommand(ctx, command);
      } finally {
         dm.getTransactionLogger().afterCommand(command);
      }
    }
    
    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
    	long start = System.nanoTime();

    	boolean isKeyLocal = dm.getLocality(command.getKey()).isLocal();
    	try {
    		return super.visitGetKeyValueCommand(ctx, command);
    	} finally {
    		if(statisticsEnabled){
    			long end = System.nanoTime();
    			if(ctx.isOriginLocal() && isKeyLocal){
    				localReadTime.addAndGet(end - start);
    				nrLocalReadOp.incrementAndGet();
    			}  
    		}
    	}
    }
    
    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics")
    public void resetStatistics() {
        super.resetStatistics();
        
        nrLocalReadOp.set(0);
        localReadTime.set(0);
    }
    
    @ManagedAttribute(description = "Number of local read commands since last reset")
    @Metric(displayName = "NrLocalReadOp", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrLocalReadOp() {
        return this.nrLocalReadOp.get();
    }
    
    @ManagedAttribute(description = "Duration of all local read command since last reset (nano-seconds)")
    @Metric(displayName = "LocalReadTime", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getLocalReadTime() {
        return localReadTime.get();
    }
}
