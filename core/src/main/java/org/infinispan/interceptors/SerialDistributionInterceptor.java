package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.context.impl.LocalTxInvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
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

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        Object retVal = invokeNextInterceptor(ctx, command);

        if (shouldInvokeRemoteTxCommand(ctx)) {

            //obtain all keys (read and write set)
            Set<Object> allKeys = new HashSet<Object>(ctx.getAffectedKeys());
            allKeys.addAll(command.getReadSet());

            //get the member to contact
            List<Address> recipients = dm.getAffectedNodes(allKeys);

            //something about L1 cache
            NotifyingNotifiableFuture<Object> f = null;
            if (isL1CacheEnabled && command.isOnePhaseCommit()) {
                f = l1Manager.flushCache(ctx.getLockedKeys(), null, null);
            }

            //send the command and wait for the vector clocks
            Map<Address, Response> responses = rpcManager.invokeRemotely(recipients, command, true, false);
            //something...
            ((LocalTxInvocationContext) ctx).remoteLocksAcquired(recipients);

            log.debugf("broadcast prepare command for transaction %s. responses are: %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    responses.toString());

            if (!responses.isEmpty()) {
                VersionVC allPreparedVC = new VersionVC();

                //process all responses
                for (Response r : responses.values()) {
                    if (r instanceof SuccessfulResponse) {
                        VersionVC preparedVC = (VersionVC) ((SuccessfulResponse) r).getResponseValue();
                        allPreparedVC.setToMaximum(preparedVC);
                        log.debugf("[%s] received response %s. all vector clock together is %s",
                                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                                preparedVC, allPreparedVC);
                    } else if(r instanceof ExceptionResponse) {
                        log.debugf("[%s] received a negative response %s",
                                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),r);
                        throw ((ExceptionResponse) r).getException();
                    } else if(!r.isSuccessful()) {
                        log.debugf("[%s] received a negative response %s",
                                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),r);
                        throw new CacheException("Unsuccessful response received... aborting transaction " +
                                Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
                    }
                }

                //this has the maximum vector clock of all
                retVal = allPreparedVC;
            }


            if (f != null) {
                f.get();
            }
        }
        return retVal;
    }
}
