package org.infinispan.mvcc;


import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author pedro
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 */
public class CommitLog {
	private static final Log log = LogFactory.getLog(CommitLog.class);

	private final AtomicReference<VersionEntry> chainOfVersion;
	private VersionVC actual;
	private final Object versionChangeNotifier = new Object();

	private boolean debug;

	private VersionVCFactory versionVCFactory;

	public CommitLog() {
		chainOfVersion = new AtomicReference<VersionEntry>(null);

	}

	@Inject
	public void inject(VersionVCFactory versionVCFactory){
		this.versionVCFactory=versionVCFactory;
	}

	//AFTER THE VersionVCFactory
	@Start(priority = 31)
	public void start() {

		VersionEntry ve = new VersionEntry();
		ve.version = this.versionVCFactory.createEmptyVersionVC();
		actual = this.versionVCFactory.createEmptyVersionVC();
		chainOfVersion.set(ve);

		debug = log.isDebugEnabled();
	}

	@Stop
	public void stop() {
		chainOfVersion.set(null);
	}

	public VersionVC getActualVersion() {
		synchronized (actual) {
			try {
				return actual.clone();
			} catch (CloneNotSupportedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				return null;
			}
		}
	}


	public VersionVC getMostRecentLessOrEqualThan(VersionVC other) {

		VersionEntry it = chainOfVersion.get();
		while(it != null) {
			if(it.version.isEmpty()) {
				return (other != null ? other : it.version);
			} else if(it.version.isBefore(other)) {
				VersionVC res=it.version;
				//res.setToMaximum(other);
				return res;
			}
			it = it.previous;
		}


		return null;



	}


	/*
    public VersionVC getMostRecentLessOrEqualThan(VersionVC other) {
        List<VersionVC> possibleVCs = new LinkedList<VersionVC>();
        VersionEntry it = chainOfVersion.get();
        while(it != null) {
            if(it.version.isEmpty()) {
                return (other.clone());
            } else if(it.version.isBefore(other)) {
                possibleVCs.add(it.version);
                if(possibleVCs.size() > 10) {
                    break;
                }
            }
            it = it.previous;
        }

        if(!possibleVCs.isEmpty()) {
            VersionVC r = (other != null) ? other.clone() : this.versionVCFactory.createVersionVC();
            for(VersionVC v : possibleVCs) {
                r.setToMaximum(v);
            }
            if(debug) {
                log.debugf("Get version less or equal than %s. return value is %s",
                        other, r);
            }
            return r;
        }

        if(debug) {
            log.debugf("Get version less or equal than %s. return value is null",
                    other);
        }
        return null;
    }

	 */

	public synchronized void addNewVersion(VersionVC other) {
		VersionEntry actualEntry;
		VersionEntry ve = new VersionEntry();
		try {
			ve.version = other.clone();
			do {
				actualEntry = chainOfVersion.get();
				ve.previous = actualEntry;
			} while(!chainOfVersion.compareAndSet(actualEntry, ve));

			synchronized (actual) {
				actual.setToMaximum(other);
				actual.notifyAll();
				/*
				log.infof("Added new version[%s] to commit log. actual version is %s",
						other, actual);
				*/
			}
		} catch (CloneNotSupportedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

	/*
    public synchronized void addNewVersion(List<VersionVC> committedTxVersion) {
        if(committedTxVersion == null || committedTxVersion.isEmpty()) {
            return ;
        }

        VersionVC toUpdateActual = new VersionVC();
        for(VersionVC other : committedTxVersion) {
            VersionEntry actualEntry;
            VersionEntry ve = new VersionEntry();
            ve.version = other.copy();

            do {
                actualEntry = chainOfVersion.get();
                ve.previous = actualEntry;
            } while(!chainOfVersion.compareAndSet(actualEntry, ve));

            toUpdateActual.setToMaximum(other);
        }

        synchronized (actual) {
            actual.setToMaximum(toUpdateActual);
            actual.notifyAll();
            if(debug) {
                log.debugf("Added news versions [%s] to commit log. actual version is %s",
                        committedTxVersion, actual);
            }
        }
    }
	 */

	
	public boolean waitUntilMinVersionIsGuaranteed(VersionVC minVersionVC, long timeout) throws InterruptedException {
		int myIndex = this.versionVCFactory.getMyIndex();
		long minVersion = minVersionVC.get(myIndex);

		if(minVersion <= 0) {
			//if(debug) {
			//    log.debugf("Wait until min version but min version is less or equals than zero");
			//}
			return true;
		}

		long finalTime = System.currentTimeMillis() + timeout;
		long newTimeout = timeout;
		do {
			
			
			newTimeout = finalTime - System.currentTimeMillis();

			synchronized (actual) {
				
				long targetValue = actual.get(myIndex);
				
				
				if(targetValue >= minVersion) {


					return true;
				}
				/*
				if(debug) {
					log.debugf("Wait until min version. actual version: %s, min version %s, position: %s, target value: %s",
							version, minVersion, position, this.versionVCFactory.translateAndGet(version, position));
				}
				
				*/
				
				
				if(newTimeout>=0){
					actual.wait(newTimeout);
				}	
			}
		} while(System.currentTimeMillis() < finalTime);
		
		
		synchronized (actual) {
			long targetValue = actual.get(myIndex);
			
			return targetValue >= minVersion;
		}

		
		

		
	}

	private static class VersionEntry {
		private  VersionVC version;
		private  VersionEntry previous;
	}
}
