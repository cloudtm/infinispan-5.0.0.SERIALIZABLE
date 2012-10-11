package org.infinispan.container.key;




/**
 * A Key that is aware about the immutability of the identified values.
 * 
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 */
public interface ContextAwareKey {
	
	/**
	 * Returns true if the value associated to this key is immutable.
	 * 
	 * @return true if the value associated to this key is immutable.
	 */
	boolean identifyImmutableValue();

}
