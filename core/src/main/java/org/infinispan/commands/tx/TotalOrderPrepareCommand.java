package org.infinispan.commands.tx;

import org.infinispan.commands.Visitor;
import org.infinispan.commands.write.WriteCommand;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.transaction.xa.GlobalTransaction;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author pedro
 *         Date: 16-06-2011
 */
public class TotalOrderPrepareCommand extends PrepareCommand {
    public static final byte COMMAND_ID = 25;
    private Map<Object, Object> writeSkewValues = null;
    private boolean isPartialReplication;

    public TotalOrderPrepareCommand(GlobalTransaction gtx, WriteCommand... modifications) {
        super(gtx, true, modifications);
    }

    public TotalOrderPrepareCommand(GlobalTransaction gtx, List<WriteCommand> commands) {
        super(gtx, commands, true);
    }

    public TotalOrderPrepareCommand() {
        super();
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
        Object[] retval = new Object[numMods + 4];
        retval[0] = globalTx;
        retval[1] = cacheName;
        retval[2] = onePhaseCommit;
        retval[3] = numMods;
        if (numMods > 0) System.arraycopy(modifications, 0, retval, 4, numMods);
        return retval;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void setParameters(int commandId, Object[] args) {
        globalTx = (GlobalTransaction) args[0];
        cacheName = (String) args[1];
        onePhaseCommit = (Boolean) args[2];
        int numMods = (Integer) args[3];
        if (numMods > 0) {
            modifications = new WriteCommand[numMods];
            System.arraycopy(args, 4, modifications, 0, numMods);
        }
    }

    public TotalOrderPrepareCommand copy() {
        TotalOrderPrepareCommand copy = new TotalOrderPrepareCommand();
        copy.globalTx = globalTx;
        copy.modifications = modifications == null ? null : modifications.clone();
        copy.onePhaseCommit = onePhaseCommit;
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
}
