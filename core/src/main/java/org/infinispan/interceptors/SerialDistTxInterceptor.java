package org.infinispan.interceptors;

import org.infinispan.CacheException;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.tx.AcquireValidationLocksCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.InvocationContextContainer;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.mvcc.CommitLog;
import org.infinispan.mvcc.CommitQueue;
import org.infinispan.mvcc.InternalMVCCEntry;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.util.Util;

import java.util.HashSet;
import java.util.Set;

/**
 * @author pruivo
 *         Date: 22/09/11
 * WARNING: this only works for put() and get(). others methods like putAll(), entrySet(), keySet()
 * and so on, it is not implemented
 */
public class SerialDistTxInterceptor extends DistTxInterceptor {
    private CommandsFactory commandsFactory;
    private CommitQueue commitQueue;
    private InvocationContextContainer icc;
    private DistributionManager distributionManager;
    private CommitLog commitLog;

    private boolean info, debug;

    @Inject
    public void inject(CommandsFactory commandsFactory, CommitQueue commitQueue, InvocationContextContainer icc,
                       DistributionManager distributionManager, CommitLog commitLog) {
        this.commandsFactory = commandsFactory;
        this.commitQueue = commitQueue;
        this.icc = icc;
        this.distributionManager = distributionManager;
        this.commitLog = commitLog;
    }

    @Start
    public void setDebugBoolean() {
        info = log.isInfoEnabled();
        debug = log.isDebugEnabled();
    }

    @Override
    public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
        if(trace) {
            log.infof("Prepare Command received for transaction %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        }

        Set<Object> writeSet = getOnlyLocalKeys(command.getAffectedKeys());
        Set<Object> readSet = getOnlyLocalKeys(command.getReadSet());

        if(ctx.isOriginLocal() && command.getAffectedKeys().isEmpty()) {
            if(debug) {
                log.debugf("Transaction [%s] is read-only and it wants to prepare. ignore it and return.",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
            }
            //read-only transaction has a valid snapshot
            return null;
        }

        if(debug) {
            log.debugf("Transaction [%s] wants to prepare. read set: %s, write set: %s, version :%s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    readSet, writeSet, command.getVersion());
        }

        //it acquires the read-write locks and it validates the read set
        AcquireValidationLocksCommand locksCommand = commandsFactory.buildAcquireValidationLocksCommand(
                command.getGlobalTransaction(), readSet, writeSet, command.getVersion());
        invokeNextInterceptor(ctx, locksCommand);

        if(debug) {
            log.debugf("Transaction [%s] passes validation",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        }

        //(this is equals to old schemes) wrap the wrote entries
        //finally, process the rest of the command
        Object retVal = super.visitPrepareCommand(ctx, command);

        VersionVC commitVC;

        if(!writeSet.isEmpty()) {
            commitVC = commitQueue.addTransaction(command.getGlobalTransaction(), ctx.calculateVersionToRead(),
                    icc.getInvocationContext().clone(), getVCPositions(writeSet));

            if(info) {
                log.infof("Transaction %s can commit. It was added to commit queue. " +
                        "The 'temporary' commit version is %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                        commitVC);
            }
        } else {
            //if it is readonly, return the most recent version
            commitVC = commitLog.getActualVersion();

            if(info) {
                log.infof("Transaction %s can commit. It is read-only on this node. " +
                        "The 'temporary' commit version is %s",
                        Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                        commitVC);
            }
        }


        if(retVal != null && retVal instanceof VersionVC) {
            //the retVal has the maximum vector clock of all involved nodes
            //this in only performed in the node that executed the transaction (I hope)
            VersionVC othersCommitVC = (VersionVC) retVal;
            commitVC.setToMaximum(othersCommitVC);
            calculateCommitVC(commitVC, getVCPositions(writeSet));
        }

        ctx.setCommitVersion(commitVC);

        if(debug) {
            log.debugf("Transaction [%s] %s commit version is %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()),
                    (ctx.isOriginLocal() ? "final" : "temporary"),
                    commitVC);
        }

        return commitVC;
    }

    @Override
    public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
        if(info) {
            log.infof("Commit Command received for transaction %s",
                    Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        }

        return super.visitCommitCommand(ctx, command);
    }

    @Override
    public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
        if(info) {
            log.infof("Rollback Command received for transaction %s", Util.prettyPrintGlobalTransaction(command.getGlobalTransaction()));
        }
        return super.visitRollbackCommand(ctx, command);
    }

    @Override
    public Object visitGetKeyValueCommand(InvocationContext ctx, GetKeyValueCommand command) throws Throwable {
        Object retVal = super.visitGetKeyValueCommand(ctx, command);

        if(ctx.isInTxScope()) {
            TxInvocationContext txctx = (TxInvocationContext) ctx;
            String gtxID = Util.prettyPrintGlobalTransaction(txctx.getGlobalTransaction());

            int pos = getPositionInVC(command.getKey());
            txctx.markReadFrom(pos);

            //update vc
            InternalMVCCEntry ime = ctx.getReadKey(command.getKey());

            if(ime == null) {
                log.warnf("InternalMVCCEntry is null for key %s in transaction %s",
                        command.getKey(), gtxID);
            } else if(txctx.hasModifications() && !ime.isMostRecent()) {
                //read an old value... the tx will abort in commit,
                //so, do not waste time and abort it now
                if(info) {
                    log.infof("Read-Write transaction [%s] read an old value. It will be aborted",
                            gtxID);
                }
                throw new CacheException("Read-Write Transaction read an old value");
            } else {
                VersionVC v = ime.getVersion();

                if(v.get(pos) == VersionVC.EMPTY_POSITION) {
                    v.set(pos,0);
                }

                if(debug) {
                    log.debugf("Transaction [%s] read key [%s] and visible vector clock is %s (Group_ID: %s)." +
                            "return value is %s",
                            gtxID, command.getKey(), v, pos,
                            ime.getValue() != null ? ime.getValue().getValue() : "null");
                }
                txctx.updateVectorClock(v);
            }
        }

        //remote get
        if(!ctx.isOriginLocal()) {
            if(debug) {
                log.debugf("Remote Get received for key %s. return value is %s. Multi Version return is %s",
                        command.getKey(), retVal, ctx.getReadKey(command.getKey()));
            }

            if(ctx.readBasedOnVersion()) {
                retVal = ctx.getReadKey(command.getKey());
            }
        }

        return retVal;
    }

    /**
     *
     * @param keys keys to check
     * @return return only the local key or empty if it has no one
     */
    private Set<Object> getOnlyLocalKeys(Set<Object> keys) {
        Set<Object> localKeys = new HashSet<Object>();
        for(Object key : keys) {
            if(distributionManager.getLocality(key).isLocal()) {
                localKeys.add(key);
            }
        }
        return localKeys;
    }

    private Integer[] getVCPositions(Set<Object> writeSet) {
        Set<Integer> positions = new HashSet<Integer>();
        for(Object key : writeSet) {
            positions.add(getPositionInVC(key));
        }
        Integer[] retVal = new Integer[positions.size()];
        positions.toArray(retVal);

        if(debug) {
            log.debugf("Groups IDs for %s are %s", writeSet, retVal);
        }

        return retVal;
    }

    private int getPositionInVC(Object key) {
        return distributionManager.locateGroup(key).getId();
    }

    private void calculateCommitVC(VersionVC vc, Integer[] writeGroups) {
        //first, calculate the maximum value in the write groups
        String originalVC = null;
        if(debug) {
            originalVC = vc.toString();
        }

        long maxValue = 0;
        for(Integer pos : writeGroups) {
            long val = vc.get(pos);
            if(val > maxValue) {
                maxValue = val;
            }
        }

        //second, set the write groups position to the maximum
        for(Integer pos : writeGroups) {
            vc.set(pos, maxValue);
        }

        if(debug) {
            log.debugf("Calculate the commit version from %s. The modified groups are %s and the final" +
                    " version is %s", originalVC, writeGroups, vc);
        }
    }
}
