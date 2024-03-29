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
package org.infinispan.distribution;

import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.control.RehashControlCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.config.Configuration;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.ConsistentHashHelper;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.jmx.annotations.MBean;
import org.infinispan.jmx.annotations.ManagedAttribute;
import org.infinispan.jmx.annotations.ManagedOperation;
import org.infinispan.loaders.CacheLoaderManager;
import org.infinispan.loaders.CacheStore;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.ReplGroup;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachemanagerlistener.CacheManagerNotifier;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.responses.ClusteredGetResponseValidityFilter;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.rpc.ResponseFilter;
import org.infinispan.remoting.rpc.ResponseMode;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.rhq.helpers.pluginAnnotations.agent.*;
import org.infinispan.container.key.ContextAwareKey;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.context.Flag.*;

/**
 * The default distribution manager implementation
 *
 * @author Manik Surtani
 * @author Vladimir Blagojevic
 * @author Mircea.Markus@jboss.com
 * @author Bela Ban
 * @since 4.0
 */
@MBean(objectName = "DistributionManager", description = "Component that handles distribution of content across a cluster")
public class DistributionManagerImpl implements DistributionManager {
    private static final Log log = LogFactory.getLog(DistributionManagerImpl.class);
    private static final boolean trace = log.isTraceEnabled();

    // Injected components
    private CacheLoaderManager cacheLoaderManager;
    private Configuration configuration;
    private RpcManager rpcManager;
    private CacheManagerNotifier notifier;
    private CommandsFactory cf;
    private TransactionLogger transactionLogger;
    private DataContainer dataContainer;
    private InterceptorChain interceptorChain;
    private InvocationContextContainer icc;
    private InboundInvocationHandler inboundInvocationHandler;
    private CacheNotifier cacheNotifier;
    private VersionVCFactory versionVCFactory;

    private final ViewChangeListener listener;
    private final ExecutorService rehashExecutor;

    // consistentHash and self are not valid in the inbound threads until
    // joinStartedLatch has been signaled by the starting thread
    // we don't have a getSelf() that waits on joinS
    private volatile ConsistentHash consistentHash;
    private Address self;
    private final CountDownLatch joinStartedLatch = new CountDownLatch(1);

    /**
     * Set if the cluster is in rehash mode, i.e. not all the nodes have applied the new state.
     */
    private volatile boolean rehashInProgress = false;
    private volatile int lastViewId = -1;

    // these fields are only used on the coordinator
    private int lastViewIdFromPushConfirmation = -1;
    private final Map<Address, Integer> pushConfirmations = new HashMap<Address, Integer>(1);
    private final Object pushConfirmationsLock = new Object();

    @ManagedAttribute(description = "If true, the node has successfully joined the grid and is considered to hold state.  If false, the join process is still in progress.")
    @Metric(displayName = "Is join completed?", dataType = DataType.TRAIT)
    private volatile boolean joinComplete = false;
    private final CountDownLatch joinCompletedLatch = new CountDownLatch(1);

    @ManagedAttribute(description = "Enables or disables the gathering of statistics by this component", writable = true)
    private boolean statisticsEnabled;
    private final AtomicLong roundTripTimeClusteredGet= new AtomicLong(0);
    private final AtomicLong nrClusteredGet= new AtomicLong(0);

    /**
     * Default constructor
     */
    public DistributionManagerImpl() {
        super();
        LinkedBlockingQueue<Runnable> rehashQueue = new LinkedBlockingQueue<Runnable>(1);
        ThreadFactory tf = new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setDaemon(true);
                t.setPriority(Thread.MIN_PRIORITY);
                t.setName("Rehasher-" + rpcManager.getTransport().getAddress());
                return t;
            }
        };
        rehashExecutor = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, rehashQueue, tf,
                new ThreadPoolExecutor.DiscardOldestPolicy());
        listener = new ViewChangeListener();
    }

    @Inject
    public void init(Configuration configuration, RpcManager rpcManager, CacheManagerNotifier notifier, CommandsFactory cf,
                     DataContainer dataContainer, InterceptorChain interceptorChain, InvocationContextContainer icc,
                     CacheLoaderManager cacheLoaderManager, InboundInvocationHandler inboundInvocationHandler,
                     CacheNotifier cacheNotifier, VersionVCFactory versionVCFactory) {
        this.cacheLoaderManager = cacheLoaderManager;
        this.configuration = configuration;
        this.rpcManager = rpcManager;
        this.notifier = notifier;
        this.cf = cf;
        this.transactionLogger = new TransactionLoggerImpl(cf);
        this.dataContainer = dataContainer;
        this.interceptorChain = interceptorChain;
        this.icc = icc;
        this.inboundInvocationHandler = inboundInvocationHandler;
        this.cacheNotifier = cacheNotifier;
        this.versionVCFactory = versionVCFactory;
    }

    // needs to be AFTER the RpcManager
    @Start(priority = 20)
    public void start() throws Exception {
        if (trace) log.trace("starting distribution manager on " + getMyAddress());
        notifier.addListener(listener);
        join();
        setStatisticsEnabled(configuration.isExposeJmxStatistics());
    }

    private int getReplCount() {
        return configuration.getNumOwners();
    }

    private Address getMyAddress() {
        return rpcManager != null ? rpcManager.getAddress() : null;
    }

    public RpcManager getRpcManager() {
        return rpcManager;
    }

    // To avoid blocking other components' start process, wait last, if necessary, for join to complete.

    @Start(priority = 1000)
    public void waitForJoinToComplete() throws Throwable {
        joinCompletedLatch.await();
        joinComplete = true;
    }

    private void join() throws Exception {
        Transport t = rpcManager.getTransport();
        List<Address> members = t.getMembers();
        self = t.getAddress();
        lastViewId = t.getViewId();
        consistentHash = ConsistentHashHelper.createConsistentHash(configuration, members);

        // allow incoming requests
        joinStartedLatch.countDown();

        // nothing to push, but we need to inform the coordinator that we have finished our push
        if (t.isCoordinator()) {
            markNodePushCompleted(t.getViewId(), t.getAddress());
        } else {
            final RehashControlCommand cmd = cf.buildRehashControlCommand(RehashControlCommand.Type.NODE_PUSH_COMPLETED, self, t.getViewId());

            // doesn't matter when the coordinator receives the command, the transport will ensure that it eventually gets there
            rpcManager.invokeRemotely(Collections.singleton(rpcManager.getTransport().getCoordinator()), cmd, false);
        }
    }

    @Stop(priority = 20)
    public void stop() {
        notifier.removeListener(listener);
        rehashExecutor.shutdownNow();
        joinStartedLatch.countDown();
        joinCompletedLatch.countDown();
        joinComplete = true;
    }


    @Deprecated
    public boolean isLocal(Object key) {
        return getLocality(key).isLocal();
    }

    public DataLocality getLocality(Object key) {
        boolean local = getConsistentHash().isKeyLocalToAddress(getSelf(), key, getReplCount());
        if (isRehashInProgress()) {
            if (local) {
                return DataLocality.LOCAL_UNCERTAIN;
            } else {
                return DataLocality.NOT_LOCAL_UNCERTAIN;
            }
        } else {
            if (local) {
                return DataLocality.LOCAL;
            } else {
                return DataLocality.NOT_LOCAL;
            }
        }
    }


    public List<Address> locate(Object key) {
        return getConsistentHash().locate(key, getReplCount());
    }

    /**
     * Hold up operations on incoming threads until the starting thread has finished initializing the consistent hash
     */
    private void waitForJoinToStart() {
        try {
            joinStartedLatch.await();
        } catch (InterruptedException e) {
            // TODO We're setting the interrupted flag so the caller can still check if the thread was interrupted, but it would be better to throw InterruptedException instead
            // The only problem is that would require a lot of method signature changes
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Thread interrupted", e);
        }
    }

    public Map<Object, List<Address>> locateAll(Collection<Object> keys) {
        return locateAll(keys, getReplCount());
    }

    public Map<Object, List<Address>> locateAll(Collection<Object> keys, int numOwners) {
        return getConsistentHash().locateAll(keys, numOwners);
    }

    public void transformForL1(CacheEntry entry) {
        if (entry.getLifespan() < 0 || entry.getLifespan() > configuration.getL1Lifespan())
            entry.setLifespan(configuration.getL1Lifespan());
    }

    public InternalCacheEntry retrieveFromRemoteSource(Object key, InvocationContext ctx) throws Exception {
        ClusteredGetCommand get = cf.buildClusteredGetCommand(key, ctx.getFlags());
        List<Address> destinations = locate(key);

        VersionVC maxToRead;
        VersionVC minVersion;

        Set<Integer> toReadFrom=new HashSet<Integer>();
        
        if(ctx.isInTxScope()) {
            

            for(Address addr : destinations) {
                toReadFrom.add(getAddressID(addr));
            }

            maxToRead = ctx.calculateVersionToRead(this.versionVCFactory);
            minVersion = ((LocalTxInvocationContext)ctx).getMinVersion();

            get.setMaxVersion(maxToRead);
            get.setMinVersion(minVersion);
            get.setAlreadyReadMask(((LocalTxInvocationContext)ctx).getReadFrom());

            if(log.isDebugEnabled()) {
                log.debugf("Perform a remote get for transaction %s. Key: %s, min version: %s, max version: %s",
                        Util.prettyPrintGlobalTransaction(((LocalTxInvocationContext) ctx).getGlobalTransaction()),
                        key, minVersion, maxToRead);
            }
        } else {
            if(log.isDebugEnabled()) {
                log.debugf("Perform a remote get in non-transactional context. key is %s", key);
            }
        }

        ResponseFilter filter = new ClusteredGetResponseValidityFilter(destinations,
                configuration.getIsolationLevel() == IsolationLevel.SERIALIZABLE);

        long start = System.nanoTime();

        Map<Address, Response> responses = rpcManager.invokeRemotely(destinations, get, ResponseMode.SYNCHRONOUS,
                configuration.getSyncReplTimeout(), false, filter);

        if(statisticsEnabled) {
            long end = System.nanoTime();
            roundTripTimeClusteredGet.addAndGet(end - start);
            nrClusteredGet.incrementAndGet();
        }


        if(ctx.isInTxScope()) {
            if(log.isDebugEnabled()) {
                log.debugf("Remote get done for transaction %s [key:%s]. response are: %s",
                        Util.prettyPrintGlobalTransaction(((LocalTxInvocationContext) ctx).getGlobalTransaction()),
                        key, responses);
            }
        } else {
            if(log.isDebugEnabled()) {
                log.debugf("Remote get done for non-transaction context [key:%s]. response are: %s", key, responses);
            }
        }

        if (!responses.isEmpty()) {
            for (Map.Entry<Address,Response> entry : responses.entrySet()) {
                Response r = entry.getValue();
                if (r == null) {
                    continue;
                }
                if (r instanceof SuccessfulResponse) {
                    if(configuration.getIsolationLevel() == IsolationLevel.SERIALIZABLE) {
                        if(ctx.isInTxScope()) {
                            InternalMVCCEntry ime = (InternalMVCCEntry) ((SuccessfulResponse) r).getResponseValue();
                            int pos = getAddressID(entry.getKey());
                            
                            
                            ctx.addRemoteReadKey(key, ime);
                            ctx.removeLocalReadKey(key);
                            	
                            
                            ((LocalTxInvocationContext) ctx).markReadFrom(versionVCFactory.translate(pos)); //To remember that this transaction has effectively read on this node

                            
                            
                            VersionVC v = ime.getVersion();
                            
                            if(this.versionVCFactory.translateAndGet(v,pos) == VersionVC.EMPTY_POSITION) {
	                            this.versionVCFactory.translateAndSet(v,pos,0);
	                        }
                            
                            
                            
                            if(log.isDebugEnabled()) {
                                log.debugf("Remote Get successful for transaction %s and key %s. Return value is %s",
                                        Util.prettyPrintGlobalTransaction(((LocalTxInvocationContext) ctx).
                                                getGlobalTransaction()), key, ime);
                            }
                            return ime.getValue();
                        } else {
                            InternalCacheValue cacheValue = (InternalCacheValue) ((SuccessfulResponse) r).getResponseValue();
                            if(log.isDebugEnabled()) {
                                log.debugf("Remote Get successful for non-transactional context and key %s. " +
                                        "Return value is %s", key, cacheValue);
                            }
                            return cacheValue.toInternalCacheEntry(key);
                        }
                    } else {
                        InternalCacheValue cacheValue = (InternalCacheValue) ((SuccessfulResponse) r).getResponseValue();
                        return cacheValue.toInternalCacheEntry(key);
                    }
                }
            }
        }

        return null;
    }

    public Address getSelf() {
        if (self == null) {
            waitForJoinToStart();
        }
        // after the waitForJoinToStart call we must see the value of self set before joinStartedLatch.countDown()
        return self;
    }

    public ConsistentHash getConsistentHash() {
        // avoid a duplicate volatile read in the common case
        ConsistentHash ch = consistentHash;
        if (ch == null) {
            waitForJoinToStart();
            // after the waitForJoinToStart call we must see the value of consistentHash set before joinStartedLatch.countDown()
            ch = consistentHash;
        }
        return ch;
    }

    public void setConsistentHash(ConsistentHash consistentHash) {
        if (trace) log.tracef("Installing new consistent hash %s", consistentHash);
        this.consistentHash = consistentHash;
    }


    @ManagedOperation(description = "Determines whether a given key is affected by an ongoing rehash, if any.")
    @Operation(displayName = "Could key be affected by rehash?")
    public boolean isAffectedByRehash(@Parameter(name = "key", description = "Key to check") Object key) {
        return isRehashInProgress() && !getConsistentHash().locate(key, getReplCount()).contains(getSelf());
    }

    public TransactionLogger getTransactionLogger() {
        return transactionLogger;
    }

    private Map<Object, InternalCacheValue> applyStateMap(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state, boolean withRetry) {
        Map<Object, InternalCacheValue> retry = withRetry ? new HashMap<Object, InternalCacheValue>() : null;
        waitForJoinToStart();

        for (Map.Entry<Object, InternalCacheValue> e : state.entrySet()) {
            if (consistentHash.locate(e.getKey(), configuration.getNumOwners()).contains(getSelf())) {
                InternalCacheValue v = e.getValue();
                InvocationContext ctx = icc.createInvocationContext();
                // locking not necessary in the case of a join since the node isn't doing anything else
                // TODO what if the node is already running?
                ctx.setFlags(CACHE_MODE_LOCAL, SKIP_CACHE_LOAD, SKIP_REMOTE_LOOKUP, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING,
                        SKIP_OWNERSHIP_CHECK);
                try {
                    PutKeyValueCommand put = cf.buildPutKeyValueCommand(e.getKey(), v.getValue(), v.getLifespan(), v.getMaxIdle(), ctx.getFlags());
                    interceptorChain.invoke(ctx, put);
                } catch (Exception ee) {
                    if (withRetry) {
                        if (trace)
                            log.tracef("Problem %s encountered when applying state for key %s. Adding entry to retry queue.", ee.getMessage(), e.getKey());
                        retry.put(e.getKey(), e.getValue());
                    } else {
                        log.problemApplyingStateForKey(ee.getMessage(), e.getKey());
                    }
                }
            } else {
                log.keyDoesNotMapToLocalNode(e.getKey(), consistentHash.locate(e.getKey(), configuration.getNumOwners()));
            }
        }
        return retry;
    }

    public void applyState(ConsistentHash consistentHash, Map<Object, InternalCacheValue> state,
                           Address sender) {
        if (trace) log.tracef("Applying new state from %s: received %d keys", sender, state.size());
        int retryCount = 3; // in case we have issues applying state.
        Map<Object, InternalCacheValue> pendingApplications = state;
        for (int i = 0; i < retryCount; i++) {
            pendingApplications = applyStateMap(consistentHash, pendingApplications, true);
            if (pendingApplications.isEmpty()) break;
        }
        // one last go
        if (!pendingApplications.isEmpty()) applyStateMap(consistentHash, pendingApplications, false);

        if(trace) log.tracef("After applying state data container has %d keys", dataContainer.size());
    }

    @Override
    public void markRehashCompleted(int viewId) {
        if (viewId < lastViewId) {
            if (trace)
                log.tracef("Ignoring old rehash completed confirmation for view %d, last view is %d", viewId, lastViewId);
            return;
        }

        if (viewId > lastViewId) {
            throw new IllegalStateException("Received rehash completed confirmation before confirming it ourselves");
        }

        if (trace) log.tracef("Rehash completed on node %s, data container has %d keys", getSelf(), dataContainer.size());
        rehashInProgress = false;
        joinCompletedLatch.countDown();
    }

    @Override
    public void markNodePushCompleted(int viewId, Address node) {
        if (!rpcManager.getTransport().isCoordinator())
            throw new IllegalStateException("Only the coordinator should handle node push completed events");

        if (trace)
            log.tracef("Coordinator: received push completed notification for %s, view id %s", node, viewId);

        // ignore all push confirmations for view ids smaller than our view id
        if (viewId < lastViewId) {
            if (log.isTraceEnabled())
                log.tracef("Coordinator: Ignoring old push completed confirmation for view %d, last view is %d", viewId, lastViewId);
            return;
        }

        synchronized (pushConfirmationsLock) {
            if (viewId < lastViewIdFromPushConfirmation) {
                if (trace)
                    log.tracef("Coordinator: Ignoring old push completed confirmation for view %d, last confirmed view is %d", viewId, lastViewIdFromPushConfirmation);
                return;
            }

            // update the latest received view id if necessary
            if (viewId > lastViewIdFromPushConfirmation) {
                lastViewIdFromPushConfirmation = viewId;
            }

            pushConfirmations.put(node, viewId);
            if (trace)
                log.tracef("Coordinator: updated push confirmations map %s", pushConfirmations);

            // the view change listener ensures that all the member nodes have an entry in the map
            for (Map.Entry<Address, Integer> pushNode : pushConfirmations.entrySet()) {
                if (pushNode.getValue() < viewId) {
                    return;
                }
            }

            if (trace)
                log.tracef("Coordinator: sending rehash completed notification for view %s", viewId);

            // all the nodes are up-to-date, broadcast the rehash completed command
            final RehashControlCommand cmd = cf.buildRehashControlCommand(RehashControlCommand.Type.REHASH_COMPLETED, getSelf(), viewId);

            // all nodes will eventually receive the command, no need to wait here
            rpcManager.broadcastRpcCommand(cmd, false);
            // The broadcast doesn't send the message to the local node
            markRehashCompleted(viewId);
        }
    }

    @Override
    public ReplGroup locateGroup(Object key) {
        return getConsistentHash().getGroupFor(key, getReplCount());
    }
    @Override
    public Set<Integer> locateGroupIds(){
    	
    	Integer myId = getConsistentHash().getHashId(self);
    	
    	Set<Address> allCaches = getConsistentHash().getCaches();
    	
    	Set<Integer> result = getBackupsIdsForNode(self, getReplCount());
    	
    	Set<Integer> backupIds;
    	
    	
    	
    	for(Address a: allCaches){
    		
    		backupIds = getBackupsIdsForNode(a, getReplCount());
    		if(backupIds.contains(myId)){
    			result.add(getConsistentHash().getHashId(a));
    		}
    		
    	}
    	
    	
    	return result;
    	
    	
    	
    	
    }
    
    private Set<Integer> getBackupsIdsForNode(Address node, int replCount){
    	Set<Integer> result = new HashSet<Integer>();
    	List<Address> backups = getConsistentHash().getBackupsForNode(node, replCount);
    	for(Address a: backups){
    		
    		result.add(getConsistentHash().getHashId(a));
    	}
    	
    	return result;
    }
    
    

    @Override
    public int getAddressID(Address addr) {
        return getConsistentHash().getHashId(addr);
    }

    @Override
    public int getSelfID() {
        return getAddressID(self);
    }

    @Listener
    public class ViewChangeListener {
        @Merged @ViewChanged
        public void handleViewChange(ViewChangedEvent e) {
            if(trace)
                log.tracef("New view received: type=%s, members: %s. Starting the RebalanceTask", e.getType(), e.getNewMembers());

            rehashInProgress = true;
            lastViewId = e.getViewId();

            if (rpcManager.getTransport().isCoordinator()) {
                // make sure the pushConfirmations map has one entry for each cluster member
                synchronized (pushConfirmationsLock) {
                    for (Address newNode : e.getNewMembers()) {
                        if (!pushConfirmations.containsKey(newNode)) {
                            if (trace)
                                log.tracef("Coordinator: adding new node %s", newNode);
                            pushConfirmations.put(newNode, -1);
                        }
                    }
                    for (Address oldNode : e.getOldMembers()) {
                        if (!e.getNewMembers().contains(oldNode)) {
                            if (trace)
                                log.tracef("Coordinator: removing node %s", oldNode);
                            pushConfirmations.remove(oldNode);
                        }
                    }
                    if (trace)
                        log.tracef("Coordinator: push confirmations list updated: %s", pushConfirmations);
                }
            }

            RebalanceTask rebalanceTask = new RebalanceTask(rpcManager, cf, configuration, dataContainer,
                    DistributionManagerImpl.this, icc, e.getViewId(), cacheNotifier);
            rehashExecutor.submit(rebalanceTask);
        }
    }

    public CacheStore getCacheStoreForRehashing() {
        if (cacheLoaderManager == null || !cacheLoaderManager.isEnabled() || cacheLoaderManager.isShared())
            return null;
        return cacheLoaderManager.getCacheStore();
    }

    @ManagedAttribute(description = "Checks whether there is a pending rehash in the cluster.")
    @Metric(displayName = "Is rehash in progress?", dataType = DataType.TRAIT)
    public boolean isRehashInProgress() {
        return rehashInProgress;
    }


    public boolean isJoinComplete() {
        return joinComplete;
    }

    public List<Address> getAffectedNodes(Collection<Object> affectedKeys) {
        if (affectedKeys == null || affectedKeys.isEmpty()) {
            if (trace) log.trace("affected keys are empty");
            return Collections.emptyList();
        }

        Set<Address> an = new HashSet<Address>();
        for (List<Address> addresses : locateAll(affectedKeys).values()) an.addAll(addresses);
        return new ArrayList<Address>(an);
    }
    
    public List<Address> getAffectedNodesAndOwners(Collection<Object> affectedKeysForNodes, Collection<Object> affectedKeysForOwners) {
    	
    	if ((affectedKeysForNodes == null || affectedKeysForNodes.isEmpty()) && (affectedKeysForOwners == null || affectedKeysForOwners.isEmpty())) {
    		if (trace) log.trace("affected keys are empty");
    		return Collections.emptyList();
    	}

    	Set<Address> an = new HashSet<Address>();

    	if(affectedKeysForNodes != null && !affectedKeysForNodes.isEmpty()){
    		for (List<Address> addresses : locateAll(affectedKeysForNodes).values()){
    			an.addAll(addresses);
    		}
    	}		

    	if(affectedKeysForOwners != null && !affectedKeysForOwners.isEmpty()){
    		for (List<Address> addresses : locateAll(affectedKeysForOwners).values()){
    			an.add(addresses.get(0));
    		}
    	}


    	return new ArrayList<Address>(an);
    }
    
    
    public void applyRemoteTxLog(List<WriteCommand> commands) {
        for (WriteCommand cmd : commands) {
            try {
                // this is a remotely originating tx
                cf.initializeReplicableCommand(cmd, true);
                InvocationContext ctx = icc.createInvocationContext();
                ctx.setFlags(SKIP_REMOTE_LOOKUP, CACHE_MODE_LOCAL, SKIP_SHARED_CACHE_STORE, SKIP_LOCKING);
                interceptorChain.invoke(ctx, cmd);
            } catch (Exception e) {
                log.exceptionWhenReplaying(cmd, e);
            }
        }

    }

    @ManagedOperation(description = "Tells you whether a given key is local to this instance of the cache.  Only works with String keys.")
    @Operation(displayName = "Is key local?")
    public boolean isLocatedLocally(@Parameter(name = "key", description = "Key to query") String key) {
        return getLocality(key).isLocal();
    }

    @ManagedOperation(description = "Locates an object in a cluster.  Only works with String keys.")
    @Operation(displayName = "Locate key")
    public List<String> locateKey(@Parameter(name = "key", description = "Key to locate") String key) {
        List<String> l = new LinkedList<String>();
        for (Address a : locate(key)) l.add(a.toString());
        return l;
    }

    @Override
    public String toString() {
        return "DistributionManagerImpl[rehashInProgress=" + rehashInProgress + ", consistentHash=" + consistentHash + "]";
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    @ManagedOperation(description = "Resets statistics gathered by this component")
    @Operation(displayName = "Reset Statistics")
    public void resetStatistics() {
        roundTripTimeClusteredGet.set(0);
        nrClusteredGet.set(0);
    }

    @Operation(displayName = "Enable/disable statistics")
    public void setStatisticsEnabled(@Parameter(name = "enabled", description = "Whether statistics should be enabled or disabled (true/false)") boolean enabled) {
        this.statisticsEnabled = enabled;
    }

    @Metric(displayName = "Statistics enabled", dataType = DataType.TRAIT)
    public boolean isStatisticsEnabled() {
        return this.statisticsEnabled;
    }

    @ManagedAttribute(description = "Round-Trip time of a clustered get key command since last reset")
    @Metric(displayName = "RoundTripTimeClusteredGetKey", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getRoundTripTimeClusteredGetKey() {
        return roundTripTimeClusteredGet.get();
    }

    @ManagedAttribute(description = "Number of clustered get key command sent since last reset")
    @Metric(displayName = "NrClusteredGet", measurementType = MeasurementType.TRENDSUP, displayType = DisplayType.SUMMARY)
    public long getNrClusteredGet() {
        return nrClusteredGet.get();
    }
}
