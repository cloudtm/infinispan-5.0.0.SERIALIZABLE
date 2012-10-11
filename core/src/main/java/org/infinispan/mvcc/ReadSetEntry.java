package org.infinispan.mvcc;



/**
 * 
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 *
 *
 */
public class ReadSetEntry {
	
	private Object key;
	
	private InternalMVCCEntry ime;
	
	public ReadSetEntry(Object key, InternalMVCCEntry ime){
		this.key = key;
		this.ime = ime;
	}

	public Object getKey() {
		return key;
	}

	public void setKey(Object key) {
		this.key = key;
	}

	public InternalMVCCEntry getIme() {
		return ime;
	}

	public void setIme(InternalMVCCEntry ime) {
		this.ime = ime;
	}
	
	

}
