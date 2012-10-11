package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.TotalOrderPrepareCommand;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.mvcc.VersionVCFactory;
import org.infinispan.remoting.responses.ExceptionResponse;
import org.infinispan.remoting.responses.Response;
import org.infinispan.remoting.responses.SuccessfulResponse;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.Util;

import java.util.Map;

/**
 * @author pedro
 *         Date: 26-08-2011
 */
public class SerialReplicationInterceptor extends ReplicationInterceptor {
	
	private VersionVCFactory versionVCFactory; 
	
	@Inject
	public void inject(VersionVCFactory versionVCFactory){
		this.versionVCFactory=versionVCFactory;
	}

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        Object retVal = invokeNextInterceptor(ctx, command);
        if (shouldInvokeRemoteTxCommand(ctx)) {
            //meaning: broadcast to everybody, in sync mode (we need the others vector clock) without
            //replication queue
            Map<Address, Response> responses = rpcManager.invokeRemotely(null, command, true, false);
            log.debugf("broadcast prepare command for transaction %s. responses are: %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    responses.toString());

            if (!responses.isEmpty()) {
                VersionVC allPreparedVC = this.versionVCFactory.createVersionVC();

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
                retVal = allPreparedVC;
            }
        }
        return retVal;
    }

    @Override
    public Object visitTotalOrderPrepareCommand(TxInvocationContext ctx, TotalOrderPrepareCommand command) throws Throwable {
        return visitPrepareCommand(ctx, command);
    }
}
