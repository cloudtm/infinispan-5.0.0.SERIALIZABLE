package org.infinispan.mvcc;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.infinispan.mvcc.exception.VersionVCDimensionException;

/**
 * VersionVC are logical vector clocks assiociated to committed <key,value> versions
 *
 *
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 */
public class VersionVC implements Externalizable, Serializable, Cloneable {
    public static final long EMPTY_POSITION = -1;
    //public static transient final VersionVC EMPTY_VERSION = new VersionVC();

    protected long[] vectorClock;
    
    private boolean isEmpty;

    
    public VersionVC(int dimension) {
        vectorClock = new long[dimension];
        for(int i=0; i<vectorClock.length; i++){
        	this.vectorClock[i]=EMPTY_POSITION;
        }
        
        this.isEmpty = true;
    }
    
    public VersionVC(){
    	this.vectorClock=null;
    	 this.isEmpty = true;
    }

    public boolean isBefore(VersionVC other) throws VersionVCDimensionException{
        if(other == null) {
            return true;
        }
        if(other.vectorClock==null){
        	return true;
        }
        if(this.vectorClock==null){
        	return true;
        }
        
        if(this.vectorClock.length!=other.vectorClock.length) throw new VersionVCDimensionException("Error: unable to compare VersionVC objects having different dimension");

        for(int i=0; i<this.vectorClock.length; i++) {
        	
            if(this.vectorClock[i]!=EMPTY_POSITION && other.vectorClock[i]!= EMPTY_POSITION && this.vectorClock[i] > other.vectorClock[i]) {
                return false;
            }
        }
        return true;
    }
    
    public boolean isBefore(VersionVC other, int referenceIndex) throws VersionVCDimensionException{
        if(other == null) {
            return true;
        }
        if(other.vectorClock==null){
        	return true;
        }
        if(this.vectorClock==null){
        	return true;
        }
        if(referenceIndex == -1){
        	return true;
        }
        
        if(this.vectorClock.length!=other.vectorClock.length) throw new VersionVCDimensionException("Error: unable to compare VersionVC objects having different dimension");
        
        if(referenceIndex >= this.vectorClock.length || referenceIndex < 0) throw new VersionVCDimensionException("Error: referenceIndex out of bounds");

        
        	
        if(this.vectorClock[referenceIndex]!=EMPTY_POSITION && other.vectorClock[referenceIndex]!= EMPTY_POSITION && this.vectorClock[referenceIndex] > other.vectorClock[referenceIndex]) {
           return false;
        }
        
        return true;
    }

   
    public boolean isAfterInPosition(VersionVC other, Integer position) throws VersionVCDimensionException{
    	
    	if(other == null) {
    		return true;
    	}
    	if(other.vectorClock == null){
    		return true;
    	}
    	if(this.vectorClock == null){
    		return false;
    	}

    	if(this.vectorClock.length!=other.vectorClock.length ) throw new VersionVCDimensionException("Error: unable to compare VersionVC objects having different dimension");

    	if(position>=this.vectorClock.length) throw new VersionVCDimensionException("Error: unable to compare VersionVC objects at position "+position);


    	return this.vectorClock[position]!=EMPTY_POSITION && other.vectorClock[position]!= EMPTY_POSITION && this.vectorClock[position] > other.vectorClock[position];
    		
    }
    
    
    public boolean isEmpty(){
    	
    	/*
    	if(this.vectorClock == null) return true;
    	
    	for(int i=0; i<this.vectorClock.length; i++){
    		if(this.vectorClock[i]!=EMPTY_POSITION){
    			return false;
    		}
    	}
    	
    	*/
    	
    	return this.isEmpty;
    }
    
    

    

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(vectorClock);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VersionVC other = (VersionVC) obj;
		if (!Arrays.equals(vectorClock, other.vectorClock))
			return false;
		return true;
	}

	
   
    public boolean equalsInPosition(VersionVC other, Integer position) throws VersionVCDimensionException{
    	if(other == null){
    		return false;
    	}
    	if(other.vectorClock == this.vectorClock) {
    		return true;
    	}
    	if(other.vectorClock == null || this.vectorClock == null){
    		return false;
    	}
    	
    	if(this.vectorClock.length!=other.vectorClock.length) throw new VersionVCDimensionException("Error: unable to compare VersionVC objects having different dimension");

    	if(position>=this.vectorClock.length) throw new VersionVCDimensionException("Error: unable to compare VersionVC objects at position "+position);


        
        return this.vectorClock[position] == other.vectorClock[position];
    	
    	
    }
    

    public long get(Integer position) throws VersionVCDimensionException{
    	
    	if(this.vectorClock == null) return EMPTY_POSITION;
    	
    	
    	if(position>=this.vectorClock.length) throw new VersionVCDimensionException("Error: unable to get value at position "+position);

        return this.vectorClock[position];
    }

    public void set(VersionVCFactory factory, Integer position, long value) throws VersionVCDimensionException{
    	
    	if(this.vectorClock == null){
     	   VersionVC newVersion= factory.createVersionVC();
     	   
     	   this.vectorClock=new long[newVersion.vectorClock.length];
            
           System.arraycopy(newVersion.vectorClock,0, this.vectorClock,0,newVersion.vectorClock.length);
     	   
        }
    	
    	if(position>=this.vectorClock.length) throw new VersionVCDimensionException("Error: unable to put value at position "+position);

        this.vectorClock[position]=value;
        
        if(this.isEmpty && value!=EMPTY_POSITION){
        	this.isEmpty = false;
        }
    }

    
    public void setToMaximum(VersionVC other) throws VersionVCDimensionException{
        if(other == null || other.vectorClock==null) {
            return;
        }
        
        if(this.vectorClock == null){
        	this.vectorClock = new long[other.vectorClock.length];
        			
        	System.arraycopy(other.vectorClock,0, this.vectorClock,0,other.vectorClock.length);
        }
        
        if(this.vectorClock.length!=other.vectorClock.length){
        	throw new VersionVCDimensionException("Error: unable to compare VersionVC objects having different dimension. This: "+this+". Other: "+other);
        }

        
        for(int i=0; i<this.vectorClock.length; i++){
        	if(this.vectorClock[i]<other.vectorClock[i]){
        		this.vectorClock[i]=other.vectorClock[i];
        		if(this.isEmpty && other.vectorClock[i] != EMPTY_POSITION){
        			this.isEmpty = false;
        		}
        	}
        }
        
    }

    public void setEmptyPositions(VersionVC other) throws VersionVCDimensionException{
    	if(other == null || other.vectorClock==null) {
            return;
        }
        
        if(this.vectorClock == null){
        	this.vectorClock = new long[other.vectorClock.length];
        			
        	System.arraycopy(other.vectorClock,0, this.vectorClock,0,other.vectorClock.length);
        }
        
        if(this.vectorClock.length!=other.vectorClock.length) throw new VersionVCDimensionException("Error: unable to compare VersionVC objects having different dimension");

        
        for(int i=0; i<this.vectorClock.length; i++){
        	if(this.vectorClock[i]==EMPTY_POSITION && other.vectorClock[i]!=EMPTY_POSITION){
        		this.vectorClock[i]=other.vectorClock[i];
        		if(this.isEmpty){
        			this.isEmpty = false;
        		}
        	}
        }
    }
    
    
    @Override
    public VersionVC clone() throws CloneNotSupportedException {
    	VersionVC dolly=(VersionVC) super.clone();
    	if(this.vectorClock!=null){
        	dolly.vectorClock=new long[this.vectorClock.length];
        
        	System.arraycopy(this.vectorClock,0, dolly.vectorClock,0,this.vectorClock.length);
    	}	
        
        return dolly;
    }

   
    public VersionVC clone(Set<Integer> pos) throws CloneNotSupportedException{
    	VersionVC dolly=(VersionVC) super.clone();
    	if(this.vectorClock!=null){
    		dolly.vectorClock=new long[this.vectorClock.length];
    	
    		for(int i=0; i<this.vectorClock.length; i++){
    			dolly.vectorClock[i]=EMPTY_POSITION;
    			dolly.isEmpty = true;
    		}
    	
    		for(Integer p : pos) {
    			if(p<this.vectorClock.length) {
    				dolly.vectorClock[p]=this.vectorClock[p];
    				if(dolly.isEmpty && this.vectorClock[p] != EMPTY_POSITION){
    					dolly.isEmpty = false;
    				}
    			}
    		}
    	}	
        return dolly;
    }
    

    @Override
    public String toString() {
    	String res="";
    	if(this.vectorClock!=null){
    		for(int i=0; i<this.vectorClock.length; i++){
    			if(i==this.vectorClock.length-1)
    				res+=i+"="+this.vectorClock[i];
    			else
    				res+=i+"="+this.vectorClock[i]+"; ";

    		}
    	}
    	return "Version{vc=" + res + "}";
    }

    public void incrementPosition(VersionVCFactory factory, Integer position) throws VersionVCDimensionException{
    	
       if(this.vectorClock == null){
    	   VersionVC newVersion= factory.createVersionVC();
    	   
    	   //this.vectorClock=new long[newVersion.vectorClock.length];
           
       	   //System.arraycopy(newVersion.vectorClock,0, this.vectorClock,0,newVersion.vectorClock.length);
    	   
    	   this.vectorClock = newVersion.vectorClock;
    	   this.isEmpty = true;
       }
        
       if(position>=this.vectorClock.length) throw new VersionVCDimensionException("Error: unable to put value at position "+position+". VersionVC: "+this);

       if(this.vectorClock[position]==EMPTY_POSITION){
    	   this.vectorClock[position] = 1L;
    	   
       }
       else{
    	   this.vectorClock[position]= this.vectorClock[position]+1;
       }
    	
       this.isEmpty = false;
        
    }

    @Override
    public void writeExternal(ObjectOutput objectOutput) throws IOException {
        if(vectorClock == null) {
            objectOutput.writeInt(0);
        }else{
        	
        	objectOutput.writeInt(this.vectorClock.length);
        	objectOutput.writeBoolean(this.isEmpty);
        	for(int i=0; i<this.vectorClock.length; i++){
        		objectOutput.writeLong(this.vectorClock[i]);
        	}
        }
   
    }

    @Override
    public void readExternal(ObjectInput objectInput) throws IOException, ClassNotFoundException {
        int size = objectInput.readInt();
        if(size == 0) {
        	this.vectorClock=null;
        	this.isEmpty = true;
        }
        else{
        	this.vectorClock=new long[size];
        	this.isEmpty = objectInput.readBoolean();
        	for(int i=0; i<size; i++){
        		this.vectorClock[i]=objectInput.readLong();
        	}
        }
    }

    

    public void clean() {
        for(int i=0; i<this.vectorClock.length;i++){
        	this.vectorClock[i]=EMPTY_POSITION;
        }
        
        this.isEmpty = true;
    }
    
    
    public static VersionVC computeMax(List<VersionVC> versions) throws VersionVCDimensionException{
    	
    	
    	Iterator<VersionVC> itr=versions.iterator();
    	
    	VersionVC current;
    	VersionVC result=null;
    	boolean first=true;
    	while(itr.hasNext()){
    		current=itr.next();
    		if(first){
    			result=new VersionVC(current.vectorClock.length);
    			first=false;
    		}
    		
    		result.setToMaximum(current);
    		
    	}
    	
    	
    	
    	return result;
    }
}
