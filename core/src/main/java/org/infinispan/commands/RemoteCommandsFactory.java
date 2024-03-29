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
package org.infinispan.commands;

import org.infinispan.CacheException;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.control.StateTransferControlCommand;
import org.infinispan.commands.module.ModuleCommandFactory;
import org.infinispan.commands.read.DistributedExecuteCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.read.MapReduceCommand;
import org.infinispan.commands.read.serializable.SerialGetKeyValueCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.remote.MultipleRpcCommand;
import org.infinispan.commands.remote.SingleRpcCommand;
import org.infinispan.commands.remote.recovery.CompleteTransactionCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTransactionsCommand;
import org.infinispan.commands.remote.recovery.GetInDoubtTxInfoCommand;
import org.infinispan.commands.remote.recovery.RemoveRecoveryInfoCommand;
import org.infinispan.commands.tx.*;
import org.infinispan.commands.write.*;
import org.infinispan.factories.GlobalComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.Map;

/**
 * Specifically used to create un-initialized {@link org.infinispan.commands.ReplicableCommand}s from a byte stream.
 * This is a {@link Scopes#GLOBAL} component and doesn't have knowledge of initializing a command by injecting
 * cache-specific components into it.
 * <p />
 * Usually a second step to unmarshalling a command from a byte stream (after
 * creating an un-initialized version using this factory) is to pass the command though {@link CommandsFactory#initializeReplicableCommand(ReplicableCommand,boolean)}.
 *
 * @see CommandsFactory#initializeReplicableCommand(ReplicableCommand,boolean)
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
@Scope(Scopes.GLOBAL)
public class RemoteCommandsFactory {
    Transport transport;
    EmbeddedCacheManager cacheManager;
    GlobalComponentRegistry registry;
    Map<Byte,ModuleCommandFactory> commandFactories;

    @Inject
    public void inject(Transport transport, EmbeddedCacheManager cacheManager, GlobalComponentRegistry registry,
                       @ComponentName(KnownComponentNames.MODULE_COMMAND_FACTORIES) Map<Byte, ModuleCommandFactory> commandFactories) {
        this.transport = transport;
        this.cacheManager = cacheManager;
        this.registry = registry;
        this.commandFactories = commandFactories;
    }

    /**
     * Creates an un-initialized command.  Un-initialized in the sense that parameters will be set, but any components
     * specific to the cache in question will not be set.
     * <p/>
     * You would typically set these parameters using {@link CommandsFactory#initializeReplicableCommand(ReplicableCommand,boolean)}
     * <p/>
     *
     * @param id         id of the command
     * @param parameters parameters to set
     * @return a replicable command
     */
    public ReplicableCommand fromStream(byte id, Object[] parameters) {
        ReplicableCommand command;
        switch (id) {
            case PutKeyValueCommand.COMMAND_ID:
                command = new PutKeyValueCommand();
                break;
            case LockControlCommand.COMMAND_ID:
                command = new LockControlCommand();
                break;
            case PutMapCommand.COMMAND_ID:
                command = new PutMapCommand();
                break;
            case RemoveCommand.COMMAND_ID:
                command = new RemoveCommand();
                break;
            case ReplaceCommand.COMMAND_ID:
                command = new ReplaceCommand();
                break;
            case GetKeyValueCommand.COMMAND_ID:
            	if(cacheManager.getDefaultConfiguration().getIsolationLevel() == IsolationLevel.SERIALIZABLE){
            		command = new SerialGetKeyValueCommand();
            	}
            	else{
            		command = new GetKeyValueCommand();
            	}
                break;
            case ClearCommand.COMMAND_ID:
                command = new ClearCommand();
                break;
            case PrepareCommand.COMMAND_ID:
                command = new PrepareCommand();
                break;
            case CommitCommand.COMMAND_ID:
                command = new CommitCommand();
                break;
            case RollbackCommand.COMMAND_ID:
                command = new RollbackCommand();
                break;
            case MultipleRpcCommand.COMMAND_ID:
                command = new MultipleRpcCommand();
                break;
            case SingleRpcCommand.COMMAND_ID:
                command = new SingleRpcCommand();
                break;
            case InvalidateCommand.COMMAND_ID:
                command = new InvalidateCommand();
                break;
            case InvalidateL1Command.COMMAND_ID:
                command = new InvalidateL1Command();
                break;
            case StateTransferControlCommand.COMMAND_ID:
                command = new StateTransferControlCommand();
                ((StateTransferControlCommand) command).init(transport);
                break;
            case ClusteredGetCommand.COMMAND_ID:
                command = new ClusteredGetCommand();
                break;
            case RehashControlCommand.COMMAND_ID:
                command = new RehashControlCommand(transport);
                break;
            case RemoveCacheCommand.COMMAND_ID:
                command = new RemoveCacheCommand(cacheManager, registry);
                break;
            case RemoveRecoveryInfoCommand.COMMAND_ID:
                command = new RemoveRecoveryInfoCommand();
                break;
            case GetInDoubtTransactionsCommand.COMMAND_ID:
                command = new GetInDoubtTransactionsCommand();
                break;
            case MapReduceCommand.COMMAND_ID:
                command = new MapReduceCommand();
                break;
            case DistributedExecuteCommand.COMMAND_ID:
                command = new DistributedExecuteCommand<Object>();
                break;
            case GetInDoubtTxInfoCommand.COMMAND_ID:
                command = new GetInDoubtTxInfoCommand();
                break;
            case CompleteTransactionCommand.COMMAND_ID:
                command = new CompleteTransactionCommand();
                break;
            //pedro
            case TotalOrderPrepareCommand.COMMAND_ID:
                command = new TotalOrderPrepareCommand();
                break;
            case VoteCommand.COMMAND_ID:
                command = new VoteCommand();
                break;
            default:
                ModuleCommandFactory mcf = commandFactories.get(id);
                if (mcf != null)
                    return mcf.fromStream(id, parameters);
                else
                    throw new CacheException("Unknown command id " + id + "!");
        }
        command.setParameters(id, parameters);
        return command;
    }
}
