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
package org.infinispan.commands.remote;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.FlagAffectedCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.entries.*;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.mvcc.CommitLog;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * Issues a remote get call.  This is not a {@link org.infinispan.commands.VisitableCommand} and hence not passed up the
 * {@link org.infinispan.interceptors.base.CommandInterceptor} chain.
 * <p/>
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public class ClusteredGetCommand extends BaseRpcCommand implements FlagAffectedCommand {

    public static final byte COMMAND_ID = 16;
    private static final Log log = LogFactory.getLog(ClusteredGetCommand.class);
    private static final boolean trace = log.isTraceEnabled();

    private Object key;

    private InvocationContextContainer icc;
    private CommandsFactory commandsFactory;
    private InterceptorChain invoker;

    private Set<Flag> flags;

    private DistributionManager distributionManager;

    private VersionVC minVersion = null; //with null, it does not waits for anything and return a value compatible with maxVC
    private VersionVC maxVersion = null; //read the most recent version (in tx context, this is not null)
    private CommitLog commitLog;

    public ClusteredGetCommand() {
    }

    public ClusteredGetCommand(Object key, String cacheName, Set<Flag> flags) {
        this.key = key;
        this.cacheName = cacheName;
        this.flags = flags;
    }

    public ClusteredGetCommand(Object key, String cacheName) {
        this(key, cacheName, Collections.<Flag>emptySet());
    }

    public void initialize(InvocationContextContainer icc, CommandsFactory commandsFactory,
                           InterceptorChain interceptorChain, DistributionManager distributionManager,
                           CommitLog commitLog) {
        this.distributionManager = distributionManager;
        this.icc = icc;
        this.commandsFactory = commandsFactory;
        this.invoker = interceptorChain;
        this.commitLog = commitLog;
    }

    /**
     * Invokes a logical "get(key)" on a remote cache and returns results.
     *
     * @param context invocation context, ignored.
     * @return returns an <code>CacheEntry</code> or null, if no entry is found.
     */
    public Object perform(InvocationContext context) throws Throwable {
        if (distributionManager != null && distributionManager.isAffectedByRehash(key)) return null;
        // make sure the get command doesn't perform a remote call
        // as our caller is already calling the ClusteredGetCommand on all the relevant nodes
        Set<Flag> commandFlags = EnumSet.of(Flag.SKIP_REMOTE_LOOKUP);
        if (this.flags != null) commandFlags.addAll(this.flags);
        GetKeyValueCommand command = commandsFactory.buildGetKeyValueCommand(key, commandFlags);
        command.setReturnCacheEntry(true);
        InvocationContext invocationContext = icc.createRemoteInvocationContextForCommand(command, getOrigin());
        boolean useMultiVersion = false;

        if(configuration.getIsolationLevel() == IsolationLevel.SERIALIZABLE && maxVersion != null) {
            useMultiVersion = true;
            invocationContext.setReadBasedOnVersion(true);
            invocationContext.setVersionToRead(maxVersion);

            long timeout = configuration.getSyncReplTimeout();
            int pos = distributionManager.getSelfID();
            if(!commitLog.waitUntilMinVersionIsGuaranteed(minVersion, pos, timeout)) {
                if(log.isInfoEnabled()) {
                    log.infof("Receive remote get request, but the value wanted is not available. key: %s," +
                            "min version: %s, max version: %s", key, minVersion, maxVersion);
                }
                return null; //no version available
            }
        }

        Object cacheEntry = invoker.invoke(invocationContext, command);

        if (cacheEntry == null) {
            if (trace) log.trace("Did not find anything, returning null");
            return null;
        }

        if(configuration.getIsolationLevel() == IsolationLevel.SERIALIZABLE && useMultiVersion) {
            InternalMVCCEntry ime = (InternalMVCCEntry) cacheEntry;
            if(log.isInfoEnabled()) {
                log.infof("Receive remote get request [key: %s, min version: %s, max version: %s] and return value is %s",
                        key, minVersion, maxVersion, ime);
            }
            return ime;
        } else {
            return getValueForWeakConsistency((CacheEntry) cacheEntry);
        }

        //TODO with serializability:
        /*
         * 1 - wait until commitLog's most recent version has a version higher than min version
         * 1.1 - wait for how much time?? (sync timeout)
         * 2 - set the max version somewhere and read the key (don't forger to return an InternalMVCCEntry)
         * 3 - send it back
         */
    }

    private InternalCacheValue getValueForWeakConsistency(CacheEntry cacheEntry) {
        //this might happen if the value was fetched from a cache loader
        if (cacheEntry instanceof MVCCEntry) {
            if (trace) log.trace("Handloing an internal cache entry...");
            MVCCEntry mvccEntry = (MVCCEntry) cacheEntry;
            return InternalEntryFactory.createValue(mvccEntry.getValue(), -1, mvccEntry.getLifespan(), -1, mvccEntry.getMaxIdle());
        } else {
            InternalCacheEntry internalCacheEntry = (InternalCacheEntry) cacheEntry;
            return internalCacheEntry.toInternalCacheValue();
        }
    }

    public byte getCommandId() {
        return COMMAND_ID;
    }

    public Object[] getParameters() {
        return new Object[]{key, cacheName, minVersion, maxVersion, flags};
    }

    public void setParameters(int commandId, Object[] args) {
        key = args[0];
        cacheName = (String) args[1];
        minVersion = (VersionVC) args[2];
        maxVersion = (VersionVC) args[3];
        if (args.length>4) {
            this.flags = (Set<Flag>) args[4];
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ClusteredGetCommand that = (ClusteredGetCommand) o;

        return !(key != null ? !key.equals(that.key) : that.key != null);
    }

    @Override
    public int hashCode() {
        int result;
        result = (key != null ? key.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return new StringBuilder()
                .append("ClusteredGetCommand{key=")
                .append(key)
                .append(", flags=").append(flags)
                .append("}")
                .toString();
    }

    public String getCacheName() {
        return cacheName;
    }

    public Object getKey() {
        return key;
    }

    @Override
    public Set<Flag> getFlags() {
        return flags;
    }

    @Override
    public void setFlags(Set<Flag> flags) {
        this.flags = flags;
    }

    public void setMinVersion(VersionVC minVersion) {
        this.minVersion = minVersion;
    }


    public void setMaxVersion(VersionVC maxVersion) {
        this.maxVersion = maxVersion;
    }
}
