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
package org.infinispan.context;


import org.infinispan.container.entries.CacheEntry;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.remoting.transport.Address;

import java.util.Set;

/**
 * A context that contains information pertaining to a given invocation.  These contexts typically have the lifespan of
 * a single invocation.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @author Mircea.Markus@jboss.com
 * @since 4.0
 */
public interface InvocationContext extends EntryLookup, FlagContainer, Cloneable {

    /**
     * Returns true if the call was originated locally, false if it is the result of a remote rpc.
     */
    boolean isOriginLocal();

    /**
     * Get the origin of the command, or null if the command originated locally
     * @return
     */
    Address getOrigin();

    /**
     * Returns true if this call is performed in the context of an transaction, false otherwise.
     */
    boolean isInTxScope();

    /**
     * Returns the in behalf of which locks will be aquired.
     */
    Object getLockOwner();

    boolean isUseFutureReturnType();

    void setUseFutureReturnType(boolean useFutureReturnType);

    InvocationContext clone();

    /**
     * Returns the set of keys that are locked for writing.
     */
    public Set<Object> getLockedKeys();

    //Pedro's new interface for the multi-version reads
    boolean readBasedOnVersion();

    void setReadBasedOnVersion(boolean value);

    void addRemoteReadKey(Object key, InternalMVCCEntry ime);
    
    void addLocalReadKey(Object key, InternalMVCCEntry ime);
    
    void removeLocalReadKey(Object key);
    
    void removeRemoteReadKey(Object key);

    InternalMVCCEntry getLocalReadKey(Object Key);
    
    InternalMVCCEntry getRemoteReadKey(Object Key);

    VersionVC calculateVersionToRead(VersionVCFactory versionVCFactory);

    VersionVC getPrepareVersion();

    void setVersionToRead(VersionVC version);
    
    void setAlreadyReadOnNode(boolean alreadyRead);
    
    boolean getAlreadyReadOnNode();
    
    void setLastReadKey(CacheEntry entry);
    
    CacheEntry getLastReadKey();
    
    void clearLastReadKey();
    
    
}
