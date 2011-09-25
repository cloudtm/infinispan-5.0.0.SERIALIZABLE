package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Start;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.NotifyingNotifiableFuture;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author pruivo
 *         Date: 23/09/11
 */
public class SerialDistributionInterceptor extends DistributionInterceptor {

    private boolean info, debug;

    @Start
    public void setLogBoolean() {
        info = log.isInfoEnabled();
        debug = log.isDebugEnabled();
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        Object retVal = invokeNextInterceptor(ctx, command);

        if (shouldInvokeRemoteTxCommand(ctx)) {
            if(info) {
                log.infof("Prepare Command received for %s and it will be multicast",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            }

            //obtain all keys (read and write set)
            Set<Object> allKeys = new HashSet<Object>(ctx.getAffectedKeys());
            allKeys.addAll(command.getReadSet());

            //get the member to contact
            List<Address> recipients = dm.getAffectedNodes(allKeys);

            if(debug) {
                log.debugf("Transaction %s accessed keys are %s and involved replicas are %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                        allKeys, recipients);
            }

            //something about L1 cache
            NotifyingNotifiableFuture<Object> f = null;
            if (isL1CacheEnabled && command.isOnePhaseCommit()) {
                f = l1Manager.flushCache(ctx.getLockedKeys(), null, null);
            }

            //send the command and wait for the vector clocks
            Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, true, false);
            //something...
            ((LocalTxInvocationContext) ctx).remoteLocksAcquired(recipients);

            if(debug) {
                log.debugf("Prepare Command multicasted for transaction %s and the responses are: %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                        responses.toString());
            }

            if (!responses.isEmpty()) {
                VersionVC allPreparedVC = new VersionVC();

                //process all responses
                for (Response r : responses.values()) {
                    if (r instanceof SuccessfulResponse) {
                        VersionVC preparedVC = (VersionVC) ((SuccessfulResponse) r).getResponseValue();
                        allPreparedVC.setToMaximum(preparedVC);
                    } else if(r instanceof ExceptionResponse) {
                        Exception e = ((ExceptionResponse) r).getException();

                        if(info) {
                            log.infof("Transaction %s received a negative response %s (reason:%s) and it must be aborted",
                                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), r,
                                    e.getLocalizedMessage());
                        }

                        throw e;
                    } else if(!r.isSuccessful()) {
                        if(info) {
                            log.debugf("Transaction %s received an unsuccessful response %s and it mus be aborted",
                                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), r);
                        }

                        throw new CacheException("Unsuccessful response received... aborting transaction " +
                                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
                    }
                }

                //this has the maximum vector clock of all
                retVal = allPreparedVC;
                if(info) {
                    log.infof("Transaction %s receive only positive votes and it can commit. Prepare version is %s",
                            Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()), allPreparedVC);
                }
            }


            if (f != null) {
                f.get();
            }
        } else {
            if(info) {
                log.infof("Prepare Command received for %s and it will *NOT* be multicast",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            }
        }
        return retVal;
    }
}
