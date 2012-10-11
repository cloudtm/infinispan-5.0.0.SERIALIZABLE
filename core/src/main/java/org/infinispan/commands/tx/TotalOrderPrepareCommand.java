package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.mvcc.VersionVC;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.*;

/**
 * @author pedro
 *         Date: 16-06-2011
 */
public class TotalOrderPrepareCommand extends PrepareCommand {
    public static final byte COMMAND_ID = 25;
    private Map<Object, Object> writeSkewValues = null;
    private boolean isPartialReplication, serializability = false;

    public TotalOrderPrepareCommand(GlobalTransaction gtx, WriteCommand... modifications) {
        super(gtx, true, modifications);
    }

    public TotalOrderPrepareCommand(GlobalTransaction gtx, List<WriteCommand> commands) {
        super(gtx, commands, true);
    }

    public TotalOrderPrepareCommand() {
        super();
    }

    public TotalOrderPrepareCommand(GlobalTransaction gtx, Object[] readSet, VersionVC version, List<WriteCommand> commands) {
        super(gtx, false, readSet, version, commands);
    }

    public Object acceptVisitor(InvocationContext ctx, Visitor visitor) throws Throwable {
        return visitor.visitTotalOrderPrepareCommand((TxInvocationContext) ctx, this);
    }

    public byte getCommandId() {
        return COMMAND_ID;
    }

    @Override
    public Object[] getParameters() {
    	int numMods = modifications == null ? 0 : modifications.length;
        int numReads = readSet == null ? 0 : readSet.length;
        Object[] retval = new Object[numMods + numReads + 6];
        retval[0] = globalTx;
        retval[1] = cacheName;
        retval[2] = onePhaseCommit;
        retval[3] = version;
        retval[4] = numMods;
        retval[5] = numReads;
        if (numMods > 0) System.arraycopy(modifications, 0, retval, 6, numMods);
        if (numReads > 0) System.arraycopy(readSet, 0, retval, 6 + numMods, numReads);
        return retval;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setParameters(int commandId, Object[] args) {
    	globalTx = (GlobalTransaction) args[0];
        cacheName = (String) args[1];
        onePhaseCommit = (Boolean) args[2];
        version = (VersionVC) args[3];
        int numMods = (Integer) args[4];
        int numReads = (Integer) args[5];
        if (numMods > 0) {
            modifications = new WriteCommand[numMods];
            System.arraycopy(args, 6, modifications, 0, numMods);
        }
        if(numReads > 0){
        	readSet = new Object[numReads];
        	System.arraycopy(args, 6 + numMods, readSet, 0, numReads);
        }	
    }

    public TotalOrderPrepareCommand copy() {
    	TotalOrderPrepareCommand copy = new TotalOrderPrepareCommand();
        copy.globalTx = globalTx;
        copy.modifications = modifications == null ? null : modifications.clone();
        copy.onePhaseCommit = onePhaseCommit;
        if(readSet != null){
        	copy.readSet = new Object[readSet.length];
        	System.arraycopy(readSet, 0, copy.readSet, 0, readSet.length);
        }
        else{
        	copy.readSet = null;
        }
        
        try {
			copy.version = version != null ? version.clone() : null;
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			copy.version=null;
		}
        return copy;
    }

    public void setOnePhaseCommit(boolean value) {
        onePhaseCommit = value;
    }

    @Override
    public String toString() {
        return "TotalOrderPrepareCommand {" +
                "gtx=" + globalTx +
                ",modifications=" + (modifications == null ? null : Arrays.asList(modifications)) +
                ",isOnePhase?=" + onePhaseCommit +
                "}";
    }

    public Map<Object, Object> getKeysAndValuesForWriteSkewCheck() {
        if(writeSkewValues == null) {
            writeSkewValues = new HashMap<Object, Object>();
            for(WriteCommand wc : modifications) {
                writeSkewValues.putAll(wc.getKeyAndValuesForWriteSkewCheck());
            }
        }
        return writeSkewValues;
    }

    public boolean isPartialReplication() {
        return isPartialReplication;
    }

    public void setPartialReplication(boolean value ){
        isPartialReplication = value;
    }

    public boolean isSerializability() {
        return serializability;
    }

    public void setSerializability(boolean serializability) {
        this.serializability = serializability;
    }
}
