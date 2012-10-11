/*
 * JBoss, Home of Professional Open Source
 * Copyright 2011 Red Hat Inc. and/or its affiliates and other
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

import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.transaction.xa.GlobalTransaction;

import javax.transaction.Transaction;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Set;

/**
 * Extension of InvocationContextFlagsOverride to be used when a TxInvocationContext
 * is required.
 * @see InvocationContextFlagsOverride
 * 
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @since 5.0
 */
public class TransactionalInvocationContextFlagsOverride extends InvocationContextFlagsOverride implements TxInvocationContext {
   
   private TxInvocationContext delegate;

   public TransactionalInvocationContextFlagsOverride(TxInvocationContext delegate, Set<Flag> flags) {
      super(delegate, flags);
      this.delegate = delegate;
   }

   @Override
   public boolean hasModifications() {
      return delegate.hasModifications();
   }

   @Override
   public Set<Object> getAffectedKeys() {
      return delegate.getAffectedKeys();
   }

   @Override
   public GlobalTransaction getGlobalTransaction() {
      return delegate.getGlobalTransaction();
   }

   @Override
   public List<WriteCommand> getModifications() {
      return delegate.getModifications();
   }

   @Override
   public void addAffectedKeys(Collection<Object> keys) {
      delegate.addAffectedKeys(keys);
   }

   @Override
   public Transaction getTransaction() {
      return delegate.getTransaction();
   }

   @Override
   public boolean isTransactionValid() {
      return delegate.isTransactionValid();
   }

    @Override
    public void addRemoteReadKey(Object key, InternalMVCCEntry ime) {
        delegate.addRemoteReadKey(key,ime);
    }
    
    @Override
    public void addLocalReadKey(Object key, InternalMVCCEntry ime) {
        delegate.addLocalReadKey(key,ime);
    }

    @Override
    public void markReadFrom(int idx) {
        delegate.markReadFrom(idx);
    }

    @Override
    public InternalMVCCEntry getLocalReadKey(Object Key) {
        return delegate.getLocalReadKey(Key);
    }
    
    @Override
    public InternalMVCCEntry getRemoteReadKey(Object Key) {
        return delegate.getRemoteReadKey(Key);
    }

    @Override
    public VersionVC calculateVersionToRead(VersionVCFactory versionVCFactory) {
        return delegate.calculateVersionToRead(versionVCFactory);
    }

    @Override
    public void updateVectorClock(VersionVC other) {
        delegate.updateVectorClock(other);
    }

    @Override
    public long getVectorClockValueIn(VersionVCFactory versionVCFactory, int idx) {
        return delegate.getVectorClockValueIn(versionVCFactory,idx);
    }
    
    @Override
    public void setVectorClockValueIn(VersionVCFactory versionVCFactory, int idx, long value) {
    	delegate.setVectorClockValueIn(versionVCFactory,idx, value);
    }

    @Override
    public VersionVC getMinVersion() {
        return delegate.getMinVersion();
    }

    @Override
    public void setCommitVersion(VersionVC version) {
        delegate.setCommitVersion(version);
    }
    
    @Override
    public void setAlreadyReadOnNode(boolean alreadyRead){
    	delegate.setAlreadyReadOnNode(alreadyRead);
    }

	@Override
	public BitSet getReadFrom() {
		return delegate.getReadFrom();
	}

}
