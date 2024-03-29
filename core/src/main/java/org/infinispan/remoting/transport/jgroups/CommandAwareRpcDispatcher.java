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
package org.infinispan.remoting.transport.jgroups;

import org.infinispan.CacheException;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.remote.ClusteredGetCommand;
import org.infinispan.commands.tx.*;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.remoting.RpcException;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.ExtendedResponse;
import org.infinispan.remoting.responses.RequestIgnoredResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.TimeoutException;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Message;
import org.jgroups.blocks.Request;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.blocks.RspFilter;
//import org.jgroups.groups.GroupAddress;
import org.jgroups.util.*;

import java.io.NotSerializableException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.infinispan.util.Util.*;

/**
 * A JGroups RPC dispatcher that knows how to deal with {@link ReplicableCommand}s.
 *
 * @author Manik Surtani (<a href="mailto:manik@jboss.org">manik@jboss.org</a>)
 * @since 4.0
 */
public class CommandAwareRpcDispatcher extends RpcDispatcher {
    ExecutorService asyncExecutor;
    InboundInvocationHandler inboundInvocationHandler;
    JGroupsDistSync distributedSync;
    long distributedSyncTimeout;
    private static final Log log = LogFactory.getLog(CommandAwareRpcDispatcher.class);
    private static final boolean trace = log.isTraceEnabled();
    private static final boolean FORCE_MCAST = Boolean.getBoolean("infinispan.unsafe.force_multicast");

    //pedro
    private boolean totalOrderBased;

    private boolean statisticsEnabled = true;

    private final AtomicLong prepareCommandBytes = new AtomicLong(0);
    private final AtomicLong commitCommandBytes = new AtomicLong(0);
    private final AtomicLong clusteredGetKeyCommandBytes = new AtomicLong(0);
    private final AtomicLong nrPrepareCommand = new AtomicLong(0);
    private final AtomicLong nrCommitCommand = new AtomicLong(0);
    private final AtomicLong nrClusteredGetKeyCommand = new AtomicLong(0);

    public CommandAwareRpcDispatcher(Channel channel,
                                     JGroupsTransport transport,
                                     ExecutorService asyncExecutor,
                                     InboundInvocationHandler inboundInvocationHandler,
                                     JGroupsDistSync distributedSync, long distributedSyncTimeout,
                                     boolean totalOrderBased) {
        super(channel, transport, transport, transport);
        this.asyncExecutor = asyncExecutor;
        this.inboundInvocationHandler = inboundInvocationHandler;
        this.distributedSync = distributedSync;
        this.distributedSyncTimeout = distributedSyncTimeout;
        this.totalOrderBased = totalOrderBased;
    }

    protected final boolean isValid(Message req) {
        if (req == null || req.getLength() == 0) {
            log.msgOrMsgBufferEmpty();
            return false;
        }

        return true;
    }

    public RspList invokeRemoteCommands(Vector<Address> dests, ReplicableCommand command, int mode, long timeout,
                                        boolean anycasting, boolean oob, RspFilter filter, boolean supportReplay, boolean asyncMarshalling,
                                        boolean broadcast)
            throws NotSerializableException, ExecutionException, InterruptedException {

        ReplicationTask task = new ReplicationTask(command, oob, dests, mode, timeout, anycasting, filter, supportReplay, broadcast);
        
/*Pedro
        boolean totalOrderCommand = command instanceof TotalOrderPrepareCommand;
        if(totalOrderCommand) {
            task.setMulticast(((TotalOrderPrepareCommand)command).isPartialReplication());
        }
        boolean vote = command instanceof VoteCommand;
        boolean noFifoNeeded =  this.totalOrderBased && (command instanceof CommitCommand ||
                command instanceof RollbackCommand ||
                command instanceof VoteCommand);

        task.setTotalOrder(totalOrderCommand);
        task.setVote(vote);
        task.setOOB(noFifoNeeded);
*/
        if (asyncMarshalling /*&& Pedro !totalOrderCommand*/) {
            asyncExecutor.submit(task);
            return null; // don't wait for a response!
        } else {
            RspList response;
            try {
                response = task.call();
            } catch (Exception e) {
                throw rewrapAsCacheException(e);
            }
            if (mode == Request.GET_NONE) return null; // "Traditional" async.
            if (response.isEmpty() || containsOnlyNulls(response))
                return null;
            else
                return response;
        }
    }

    private boolean containsOnlyNulls(RspList l) {
        for (Rsp r : l.values()) {
            if (r.getValue() != null || !r.wasReceived() || r.wasSuspected()) return false;
        }
        return true;
    }

    /**
     * Message contains a Command. Execute it against *this* object and return result.
     */
    @Override
    public Object handle(Message req) {
        if (isValid(req)) {
            try {
                ReplicableCommand cmd = (ReplicableCommand) req_marshaller.objectFromByteBuffer(req.getBuffer(), req.getOffset(), req.getLength());
                if (cmd instanceof CacheRpcCommand)
                    return executeCommand((CacheRpcCommand) cmd, req);
                else
                    return cmd.perform(null);
            } catch (Throwable x) {
                if (trace) log.trace("Problems invoking command.", x);
                return new ExceptionResponse(new CacheException("Problems invoking command.", x));
            }
        } else {
            return null;
        }
    }

    private Response executeCommand(CacheRpcCommand cmd, Message req) throws Throwable {
        if (cmd == null) throw new NullPointerException("Unable to execute a null command!  Message was " + req);
        if (trace) log.tracef("Attempting to execute command: %s [sender=%s]", cmd, req.getSrc());
        return inboundInvocationHandler.handle(cmd, JGroupsTransport.fromJGroupsAddress(req.getSrc()));
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + "[Outgoing marshaller: " + req_marshaller + "; incoming marshaller: " + rsp_marshaller + "]";
    }

    private class ReplicationTask implements Callable<RspList> {

        private ReplicableCommand command;
        private boolean oob;
        private Vector<Address> dests;
        private int mode;
        private long timeout;
        private boolean anycasting;
        private RspFilter filter;
        boolean supportReplay = false;
        boolean broadcast = false;

        //PEDRO
        boolean totalOrder = false; //for replication based on atomic broadcast
        boolean vote = false; //for partial replication scheme
        boolean multicast = false;

        private ReplicationTask(ReplicableCommand command, boolean oob, Vector<Address> dests,
                                int mode, long timeout,
                                boolean anycasting, RspFilter filter, boolean supportReplay, boolean broadcast) {
            this.command = command;
            this.oob = oob;
            this.dests = dests;
            this.mode = mode;
            this.timeout = timeout;
            this.anycasting = anycasting;
            this.filter = filter;
            this.supportReplay = supportReplay;
            this.broadcast = broadcast;
        }

        private Message constructMessage(Buffer buf, Address recipient) {
            Message msg = new Message();
            msg.setBuffer(buf);
            if (oob) msg.setFlag(Message.OOB);
            if (mode != Request.GET_NONE) {
                msg.setFlag(Message.DONT_BUNDLE);
                msg.setFlag(Message.NO_FC);
            }
            msg.setFlag(Message.NO_TOTAL_ORDER);
            if (recipient != null) {
                msg.setDest(recipient);
            }
            return msg;
        }

        private Buffer marshallCall() {
            Buffer buf;
            try {
                buf = req_marshaller.objectToBuffer(command);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("Failure to marshal argument(s)", e);
            }

            if(statisticsEnabled) {
                if(command instanceof PrepareCommand) {
                    prepareCommandBytes.addAndGet(buf.getLength());
                    nrPrepareCommand.incrementAndGet();
                } else if(command instanceof CommitCommand) {
                    commitCommandBytes.addAndGet(buf.getLength());
                    nrCommitCommand.incrementAndGet();
                } else if(command instanceof ClusteredGetCommand) {
                    clusteredGetKeyCommandBytes.addAndGet(buf.getLength());
                    nrClusteredGetKeyCommand.incrementAndGet();
                }
            }

            return buf;
        }

        public void setTotalOrder(boolean value) {
            this.totalOrder = value;
        }

        public void setVote(boolean vote) {
            this.vote = vote;
        }

        public void setMulticast(boolean multicast) {
            this.multicast = multicast;
        }

        public void setOOB(boolean value) {
            oob |= value;
        }

        public RspList call() throws Exception {
            if (trace) log.tracef("Replication task sending %s to addresses %s", command, dests);

            // Replay capability requires responses from all members!
            int mode = supportReplay ? Request.GET_ALL : this.mode;

            if (filter != null) mode = Request.GET_FIRST;

            RspList retval = null;
            Buffer buf;
            //Pedro -- added multicast e abcast condition
            /*
            if(totalOrder) {
                RequestOptions opts = new RequestOptions();
                if(((TotalOrderPrepareCommand)command).isSerializability()) {
                    opts.setMode(Request.GET_ALL);
                } else {
                    opts.setMode(Request.GET_NONE);
                }
                opts.setTimeout(timeout);
                opts.setRspFilter(filter);
                opts.setAnycasting(false);
                buf = marshallCall();

                //if dests is null, then send an atomic broadcast message. otherwise send a group multicast msg
                GroupAddress gaddr = null;
                Set<Address> target = null;
                if(dests != null && multicast) {
                    gaddr = new GroupAddress();
                    gaddr.addAllAddress(dests);
                    target = new HashSet<Address>(dests);
                }

                //don't bundle and don't pass through flow control
                Message msg = constructMessage(buf, gaddr);
                //msg.setFlag(Message.DONT_BUNDLE);
                //msg.setFlag(Message.NO_FC);
                msg.clearFlag(Message.NO_TOTAL_ORDER); //we want total order...

                if(vote) {
                    msg.setFlag(Message.NO_TOTAL_ORDER);
                    msg.setFlag(Message.OOB);
                }

                retval = castMessage(target, msg , opts);
            } else */if (broadcast || FORCE_MCAST) {
                RequestOptions opts = new RequestOptions();
                opts.setMode(mode);
                opts.setTimeout(timeout);
                opts.setRspFilter(filter);
                opts.setAnycasting(false);
                buf = marshallCall();

                //pedro -- remove myself from list
                opts.setExclusionList(channel.getAddress());

                retval = castMessage(dests, constructMessage(buf, null), opts);
            } else {
                Set<Address> targets = new HashSet<Address>(dests); // should sufficiently randomize order.
                RequestOptions opts = new RequestOptions();
                opts.setMode(mode);
                opts.setTimeout(timeout);

                targets.remove(channel.getAddress()); // just in case
                if (targets.isEmpty()) return new RspList();
                buf = marshallCall();

                // if at all possible, try not to use JGroups' ANYCAST for now.  Multiple (parallel) UNICASTs are much faster.
                if (filter != null) {
                    // This is possibly a remote GET.
                    // These UNICASTs happen in parallel using sendMessageWithFuture.  Each future has a listener attached
                    // (see FutureCollator) and the first successful response is used.
                    FutureCollator futureCollator = new FutureCollator(filter, targets.size(), timeout);
                    for (Address a : targets) {
                        NotifyingFuture<Object> f = sendMessageWithFuture(constructMessage(buf, a), opts);
                        futureCollator.watchFuture(f, a);
                    }
                    retval = futureCollator.getResponseList();
                } else if (mode == Request.GET_ALL) {
                    // A SYNC call that needs to go everywhere
                    Map<Address, Future<Object>> futures = new HashMap<Address, Future<Object>>(targets.size());

                    for (Address dest : targets) futures.put(dest, sendMessageWithFuture(constructMessage(buf, dest), opts));

                    retval = new RspList();



                    // a get() on each future will block till that call completes.
                    for (Map.Entry<Address, Future<Object>> entry : futures.entrySet()) {
                        try {
                            retval.addRsp(entry.getKey(), entry.getValue().get(timeout, MILLISECONDS));
                        } catch (java.util.concurrent.TimeoutException te) {
                            throw new TimeoutException(formatString("Timed out after %s waiting for a response from %s",
                                    prettyPrintTime(timeout), entry.getKey()));
                        }
                    }



                } else if (mode == Request.GET_NONE) {
                    // An ASYNC call.  We don't care about responses.
                    for (Address dest : targets) sendMessage(constructMessage(buf, dest), opts);
                }
            }

            // we only bother parsing responses if we are not in ASYNC mode.
            if (mode != Request.GET_NONE) {

                if (trace) log.tracef("Responses: %s", retval);

                // a null response is 99% likely to be due to a marshalling problem - we throw a NSE, this needs to be changed when
                // JGroups supports http://jira.jboss.com/jira/browse/JGRP-193
                // the serialization problem could be on the remote end and this is why we cannot catch this above, when marshalling.
                if (retval == null)
                    throw new NotSerializableException("RpcDispatcher returned a null.  This is most often caused by args for "
                            + command.getClass().getSimpleName() + " not being serializable.");

                if (supportReplay) {
                    boolean replay = false;
                    Vector<Address> ignorers = new Vector<Address>();
                    for (Map.Entry<Address, Rsp> entry : retval.entrySet()) {
                        Object value = entry.getValue().getValue();
                        if (value instanceof RequestIgnoredResponse) {
                            ignorers.add(entry.getKey());
                        } else if (value instanceof ExtendedResponse) {
                            ExtendedResponse extended = (ExtendedResponse) value;
                            replay |= extended.isReplayIgnoredRequests();
                            entry.getValue().setValue(extended.getResponse());
                        }
                    }

                    if (replay && !ignorers.isEmpty()) {
                        Message msg = constructMessage(buf, null);
                        //Since we are making a sync call make sure we don't bundle
                        //See ISPN-192 for more details
                        msg.setFlag(Message.DONT_BUNDLE);

                        if (trace)
                            log.tracef("Replaying message to ignoring senders: %s", ignorers);
                        RequestOptions opts = new RequestOptions();
                        opts.setMode(Request.GET_ALL);
                        opts.setTimeout(timeout);
                        opts.setAnycasting(anycasting);
                        opts.setRspFilter(filter);
                        RspList responses = castMessage(ignorers, msg, opts);
                        if (responses != null)
                            retval.putAll(responses);
                    }
                }
            }

            return retval;
        }
    }

    static class SenderContainer {
        final Address address;
        volatile boolean processed = false;

        SenderContainer(Address address) {
            this.address = address;
        }

        @Override
        public String toString() {
            return "Sender{" +
                    "address=" + address +
                    ", responded=" + processed +
                    '}';
        }
    }

    class FutureCollator implements FutureListener<Object> {
        final RspFilter filter;
        volatile RspList retval;
        final Map<Future<Object>, SenderContainer> futures = new HashMap<Future<Object>, SenderContainer>(4);
        volatile Exception exception;
        volatile int expectedResponses;
        final long timeout;

        FutureCollator(RspFilter filter, int expectedResponses, long timeout) {
            this.filter = filter;
            this.expectedResponses = expectedResponses;
            this.timeout = timeout;
        }

        public void watchFuture(NotifyingFuture<Object> f, Address address) {
            futures.put(f, new SenderContainer(address));
            f.setListener(this);
        }

        public RspList getResponseList() throws Exception {
            long giveupTime = System.currentTimeMillis() + timeout;
            boolean notTimedOut = true;
            synchronized (this) {
                while (notTimedOut && expectedResponses > 0 && retval == null) {
                    notTimedOut = giveupTime > System.currentTimeMillis();
                    this.wait(timeout);
                }
            }

            // if we've got here, we either have the response we need or aren't expecting any more responses - or have run out of time.
            if (retval != null)
                return retval;
            else if (exception != null)
                throw exception;
            else if (notTimedOut)
                throw new RpcException(format("No more valid responses.  Received invalid responses from all of %s", futures.values()));
            else
                throw new TimeoutException(format("Timed out waiting for %s for valid responses from any of %s.", Util.prettyPrintTime(timeout), futures.values()));
        }

        @Override
        @SuppressWarnings("unchecked")
        public void futureDone(Future<Object> objectFuture) {
            synchronized (this) {
                SenderContainer sc = futures.get(objectFuture);
                if (sc.processed) {
                    // This can happen - it is a race condition in JGroups' NotifyingFuture.setListener() where a listener
                    // could be notified twice.
                    if (trace) log.tracef("Not processing callback; already processed callback for sender %s", sc.address);
                } else {
                    sc.processed = true;
                    Address sender = sc.address;
                    try {
                        if (retval == null) {
                            Object response = objectFuture.get();
                            if (trace) log.tracef("Received response: %s from %s", response, sender);
                            filter.isAcceptable(response, sender);
                            if (!filter.needMoreResponses())
                                retval = new RspList(Collections.singleton(new Rsp(sender, response)));
                        } else {
                            if (log.isTraceEnabled())
                                log.tracef("Skipping response from %s since a valid response for this request has already been received", sender);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (ExecutionException e) {
                        if (e.getCause() instanceof org.jgroups.TimeoutException)
                            exception = new TimeoutException("Timeout!", e);
                        else if (e.getCause() instanceof Exception)
                            exception = (Exception) e.getCause();
                        else
                            log.info("Caught a Throwable.", e.getCause());

                        if (log.isDebugEnabled())
                            log.debugf("Caught exception %s from sender %s.  Will skip this response.", exception.getClass().getName(), sender);
                        if (trace) log.trace("Exception caught: ", exception);
                    } finally {
                        expectedResponses--;
                        this.notify();
                    }
                }
            }
        }
    }

    public void setStatisticsEnabled(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }


    public void resetStatistics() {
        prepareCommandBytes.set(0);
        commitCommandBytes.set(0);
        clusteredGetKeyCommandBytes.set(0);
        nrPrepareCommand.set(0);
        nrCommitCommand.set(0);
        nrClusteredGetKeyCommand.set(0);
    }

    public long getPrepareCommandBytes() {
        return prepareCommandBytes.get();
    }

    public long getCommitCommandBytes() {
        return commitCommandBytes.get();
    }

    public long getClusteredGetKeyCommandBytes() {
        return clusteredGetKeyCommandBytes.get();
    }

    public long getNrPrepareCommand() {
        return nrPrepareCommand.get();
    }

    public long getNrCommitCommand() {
        return nrCommitCommand.get();
    }

    public long getNrClusteredGetKeyCommand() {
        return nrClusteredGetKeyCommand.get();
    }
}

