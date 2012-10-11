package org.infinispan.commands.read.serializable;

import java.util.Set;

import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.notifications.cachelistener.CacheNotifier;

/**
 *
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 *
 * 
 */
public class SerialGetKeyValueCommand extends GetKeyValueCommand {

	public SerialGetKeyValueCommand(Object key, CacheNotifier notifier,
			Set<Flag> flags) {
		super(key, notifier, flags);
		
	}

	public SerialGetKeyValueCommand() {
		super();
	}
	
	
	@Override
	public Object perform(InvocationContext ctx) throws Throwable {
		CacheEntry entry = ctx.getLastReadKey();
		
		

		if (entry == null || entry.isNull()) {
			if (trace) {
				log.trace("Entry not found");
			}
			return null;
		}
		if (entry.isRemoved()) {
			if (trace) {
				log.tracef("Entry has been deleted and is of type %s", entry.getClass().getSimpleName());
			}
			return null;
		}
		final Object value = entry.getValue();
		// FIXME: There's no point in notifying twice.
		notifier.notifyCacheEntryVisited(key, value, true, ctx);
		final Object result = returnCacheEntry ? entry : value;
		if (trace) {
			log.tracef("Found value %s", result);
		}
		notifier.notifyCacheEntryVisited(key, value, false, ctx);
		return result;
	}



}
