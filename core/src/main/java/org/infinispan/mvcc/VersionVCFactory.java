package org.infinispan.mvcc;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;


import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.notifications.cachelistener.annotation.TopologyChanged;
import org.infinispan.notifications.cachelistener.event.TopologyChangedEvent;
import org.infinispan.notifications.cachemanagerlistener.annotation.Merged;
import org.infinispan.notifications.cachemanagerlistener.annotation.ViewChanged;
import org.infinispan.notifications.cachemanagerlistener.event.ViewChangedEvent;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

/**
 * Factory for building VersionVC objects
 *
 *
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 */
public class VersionVCFactory {
	
	private static final Log log = LogFactory.getLog(VersionVCFactory.class);
	
	
	private boolean distribution;
	
	private DistributionManager distributionManager;
	private RpcManager rpcManager;
	private EmbeddedCacheManager cacheManager;
	private CacheNotifier cacheNotifier;
	private VersionVCFactoryListener listener;
	
	
	private ConcurrentHashMap<Integer, Integer> translationTable;
	
	private int myIndex;
	
	private Set<Integer> group;
	
	public VersionVCFactory(boolean distribution){
		this.distribution = distribution;
	
		this.group = new HashSet<Integer>();
		
		if(distribution){
			this.listener=new VersionVCFactoryListener(this);
			this.myIndex = -1;
		}
		else{
			this.myIndex = 0;
		}
		
	}
	
	
	
	@Inject
    public void inject(DistributionManager distributionManager, RpcManager rpcManager, EmbeddedCacheManager cacheManager, CacheNotifier cacheNotifier) {
        this.distributionManager=distributionManager;
        this.rpcManager=rpcManager;
        this.cacheManager=cacheManager;
        this.cacheNotifier=cacheNotifier;
    }

	//AFTER THE DistributionManagerImpl
    @Start(priority = 30)
    public void start() {
    	
    	if(distribution){
    	
    	this.translationTable=new ConcurrentHashMap<Integer, Integer>();
    	
    	updateTranslation(this.distributionManager.getConsistentHash().getCaches());
    	
    	
    	if(log.isDebugEnabled()){
			   String res="NEW MAPPING"+'\n';
			   Iterator<Entry<Integer,Integer>> itrEntry=this.translationTable.entrySet().iterator();
			   Entry<Integer,Integer> currentEntry;
			   while(itrEntry.hasNext()){
				   currentEntry=itrEntry.next();
				   res+="["+currentEntry.getKey()+"; "+currentEntry.getValue()+"]";
			   }
			   
			   log.debug(res);
		   }
    	
    	this.cacheManager.addListener(this.listener);
    	this.cacheNotifier.addListener(this.listener);
    	
    	}
        
    }
    
    public void updateTranslation(Set<Address> addresses){
    	
    	Iterator<Address> itr=addresses.iterator();
		   Address current;
		   TreeSet<Integer> set=new TreeSet();
		   while(itr.hasNext()){
			   current=itr.next();
			   set.add(this.distributionManager.getAddressID(current));
		   }
		   
		   Iterator<Integer> itrInt=set.iterator();
		   int i=0;
		   while(itrInt.hasNext()){
			   this.translationTable.put(itrInt.next(), i);
			   i++;
		   }
		  
		   
		   this.myIndex = this.translationTable.get(this.distributionManager.getSelfID());
		   
		   
		   
		   Set<Integer> groupForDM = this.distributionManager.locateGroupIds();
		   
		   
		   this.group=new HashSet<Integer>();
		   
		   for(Integer dmIndex: groupForDM){
			   this.group.add(this.translationTable.get(dmIndex));
		   }
		   
		   if(log.isDebugEnabled()){
			   log.debug("MyIndex: "+this.myIndex);
			   String res="Group:";
			   for(Integer index: this.group){
				   res+=" "+index;
			   }
			   
			   log.debug(res);
		   }
		   
    }
    
    public VersionVC createVersionVC(){
    	if(distribution){
    		return new VersionVC(this.translationTable.size());
    	}
    	else{
    		return new VersionVC(1);
    	}
    }
    
    public VersionVC createEmptyVersionVC(){
    	
    	return new VersionVC();
    }
    
    public VersionVC createVisibleVersionVC(VersionVC version, BitSet alreadyRead){
    	
    	VersionVC result = null;
    	
    	if(version != null){
    		 result = createVersionVC();
    		 
    		 for(int i=0; i< result.vectorClock.length; i++){
    			 
    			 if((version.vectorClock[i] != VersionVC.EMPTY_POSITION) && alreadyRead.get(i)){
    				 result.vectorClock[i] = version.vectorClock[i];
    			 }	 
    		 }
    	}
    	
    	return result;
    }
    
    public long translateAndGet(VersionVC version, int position){
    	if(this.distribution){
    		return version.get(this.translationTable.get(position));
    	}
    	else{
    		return version.get(0);
    	}	
    }
    
    public void translateAndSet(VersionVC version, int position, long value){
    	if(this.distribution){
    		version.set(this, this.translationTable.get(position),value);
    	}
    	else{
    		version.set(this, 0,value);
    	}
    }
    
    public boolean translateAndIsAfterInPosition(VersionVC version1,VersionVC version2,int position){
    	if(this.distribution){
    		return version1.isAfterInPosition(version2,this.translationTable.get(position));
    	}
    	else{
    		return version1.isAfterInPosition(version2,0);
    	}
    }
    
    public boolean translateAndEqualsInPosition(VersionVC version1,VersionVC version2,int position){
    	if(this.distribution){
    		return version1.equalsInPosition(version2,this.translationTable.get(position));
    	}
    	else{
    		return version1.equalsInPosition(version2,0);
    	}
    }
    
    public void translateAndIncrementPosition(VersionVC version, Integer position){
    	
    	version.incrementPosition(this, this.translationTable.get(position));
    }
    
    public VersionVC translateAndClone(VersionVC version, Set<Integer> positions)throws CloneNotSupportedException{
    	if(this.distribution){
    		Set<Integer> translatedPositions=new HashSet<Integer>();
    		Iterator<Integer> itr=positions.iterator();
    		Integer current;
    		Integer translation;
    		while(itr.hasNext()){
    			current=itr.next();
    			translation=this.translationTable.get(current);
    			if(translation!=null){
    				translatedPositions.add(translation);
    			}
    		}

    		return version.clone(translatedPositions);    	
    	}
    	else{

    		return version.clone();
    	}
    }
    
    public int translate(int nodeId){
    	
    	return this.translationTable.get(nodeId);
    }
    
    public int getMyIndex(){
    	
    	return this.myIndex;
    }
    
    public Set<Integer> getGroupIndexes(){
    	return this.group;
    }
    
    @Listener
    public class VersionVCFactoryListener {

       private VersionVCFactory vf;
       
       public VersionVCFactoryListener(VersionVCFactory vf){
    	   this.vf=vf;
       }
    
       @Merged @ViewChanged
       public void newView(ViewChangedEvent e) {
    	   
    	  if(log.isDebugEnabled()){
    		  String res="NEW VIEW: "+e.getViewId()+'\n'+"OLD_MEMBERS:"+'\n';
    		  Iterator<Address> itr=e.getOldMembers().iterator();
    		  Address current;
    		  while(itr.hasNext()){
    			  current=itr.next();
    			  res+=""+current+"->"+this.vf.distributionManager.getAddressID(current)+'\n';
    		  }
    		  
    		  res+="NEW_MEMBERS:"+'\n';
    		  
    		  itr=e.getNewMembers().iterator();
    		  
    		  while(itr.hasNext()){
    			  current=itr.next();
    			  res+=""+current+"->"+this.vf.distributionManager.getAddressID(current)+'\n';
    		  }
    		  
    		  log.debug(res);
    	  }
    	 
         
       }
       
       @TopologyChanged
       public void newTopology(TopologyChangedEvent e){
    	   if(!e.isPre()){
    		   if(log.isDebugEnabled()){


    			   String res="NEW TOPOLOGY: "+'\n';
    			   Iterator<Address> itr=e.getConsistentHashAtEnd().getCaches().iterator();
    			   Address current;
    			   while(itr.hasNext()){
    				   current=itr.next();
    				   res+=""+current+"->"+this.vf.distributionManager.getAddressID(current)+'\n';
    			   }



    			   log.debug(res);

    		   }
    		   
    		   this.vf.updateTranslation(e.getConsistentHashAtEnd().getCaches());
    		   
			   
			   
			   if(log.isDebugEnabled()){
				   String res="NEW MAPPING"+'\n';
				   Iterator<Entry<Integer,Integer>> itrEntry=this.vf.translationTable.entrySet().iterator();
				   Entry<Integer,Integer> currentEntry;
				   while(itrEntry.hasNext()){
					   currentEntry=itrEntry.next();
					   res+="["+currentEntry.getKey()+"; "+currentEntry.getValue()+"]";
				   }
				   
				   log.debug(res);
			   }
    		   
    		   
    	   }

       }

       
    }
    
   
}
