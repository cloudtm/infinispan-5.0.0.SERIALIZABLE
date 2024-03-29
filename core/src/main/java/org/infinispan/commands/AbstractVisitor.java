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

import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.read.*;
import org.infinispan.commands.tx.*;
import org.infinispan.commands.write.*;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;

import java.util.Collection;

/**
 * An abstract implementation of a Visitor that delegates all visit calls to a default handler which can be overridden.
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 * @author Galder Zamarreño
 * @since 4.0
 */
public abstract class AbstractVisitor implements Visitor {
    // write commands

    public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitRemoveCommand(InvocationContext ctx, RemoveCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitReplaceCommand(InvocationContext ctx, ReplaceCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitClearCommand(InvocationContext ctx, ClearCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitPutMapCommand(InvocationContext ctx, PutMapCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitEvictCommand(InvocationContext ctx, EvictCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    // read commands

    public Object visitSizeCommand(InvocationContext ctx, SizeCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitKeySetCommand(InvocationContext ctx, KeySetCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitValuesCommand(InvocationContext ctx, ValuesCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitEntrySetCommand(InvocationContext ctx, EntrySetCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    // tx commands

    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitInvalidateCommand(InvocationContext ctx, InvalidateCommand invalidateCommand) throws Throwable {
        return handleDefault(ctx, invalidateCommand);
    }

    public Object visitInvalidateL1Command(InvocationContext ctx, InvalidateL1Command invalidateL1Command) throws Throwable {
        return visitInvalidateCommand(ctx, invalidateL1Command);
    }

    /**
     * A default handler for all commands visited.  This is called for any visit method called, unless a visit command is
     * appropriately overridden.
     *
     * @param ctx     invocation context
     * @param command command to handle
     * @return return value
     * @throws Throwable in the case of a problem
     */
    protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
        return null;
    }

    /**
     * Helper method to visit a collection of VisitableCommands.
     *
     * @param ctx     Invocation context
     * @param toVisit collection of commands to visit
     * @throws Throwable in the event of problems
     */
    public void visitCollection(InvocationContext ctx, Collection<? extends VisitableCommand> toVisit) throws Throwable {
        for (VisitableCommand command : toVisit) {
            command.acceptVisitor(ctx, this);
        }
    }

    public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitUnknownCommand(InvocationContext ctx, VisitableCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    public Object visitDistributedExecuteCommand(InvocationContext ctx, DistributedExecuteCommand<?> command) throws Throwable {
        return handleDefault(ctx, command);
    }

    //PEDRO
    public Object visitTotalOrderPrepareCommand(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    @Override
    public Object visitVoteCommand(TxInvocationContext ctx, VoteCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }

    @Override
    public Object visitAcquireValidationLocksCommand(TxInvocationContext ctx, AcquireValidationLocksCommand command) throws Throwable {
        return handleDefault(ctx, command);
    }
}