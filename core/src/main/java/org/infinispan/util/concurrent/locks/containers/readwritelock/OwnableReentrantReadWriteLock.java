package org.infinispan.util.concurrent.locks.containers.readwritelock;

import org.infinispan.context.InvocationContextContainer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author pedro
 *         Date: 25-07-2011
 */
public class OwnableReentrantReadWriteLock implements ReadWriteLock {
    private Map<Object, Integer> readers;
    private volatile Object writer;
    private int writeAccesses;

    private final Object mutex = new Object();

    private transient InvocationContextContainer icc;

    private final Lock readLock = new Lock() {
        @Override
        public void lock() {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                while(!canGrantReadAccess(requestor)) {
                    try {
                        mutex.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
                int accesses = getReadAccessCount(requestor) + 1;
                readers.put(requestor, accesses);
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                while(!canGrantReadAccess(requestor)) {
                    mutex.wait();
                }
                int accesses = getReadAccessCount(requestor) + 1;
                readers.put(requestor, accesses);
            }
        }

        @Override
        public boolean tryLock() {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                if(canGrantReadAccess(requestor)) {
                    int accesses = getReadAccessCount(requestor) + 1;
                    readers.put(requestor, accesses);
                    return true;
                } else {
                    return false;
                }
            }
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                if(canGrantReadAccess(requestor)) {
                    int accesses = getReadAccessCount(requestor) + 1;
                    readers.put(requestor, accesses);
                    return true;
                }
                mutex.wait(timeUnit.toMillis(l),0);
                if(canGrantReadAccess(requestor)) {
                    int accesses = getReadAccessCount(requestor) + 1;
                    readers.put(requestor, accesses);
                    return true;
                } else {
                    return false;
                }
            }
        }

        @Override
        public void unlock() {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                if(!readers.containsKey(requestor)) {
                    return ;// throw new IllegalMonitorStateException() //pshiiuuu!!
                }
                int i = getReadAccessCount(requestor) - 1;
                if(i == 0) {
                    readers.remove(requestor);
                } else {
                    readers.put(requestor,i);
                }
                mutex.notifyAll();
            }
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    };

    private final Lock writeLock = new Lock() {
        @Override
        public void lock() {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                while(!canGrantWriteAccess(requestor)) {
                    try {
                        mutex.wait();
                    } catch (InterruptedException e) {
                        //ignore
                    }
                }
                writeAccesses++;
                writer = requestor;
            }
        }

        @Override
        public void lockInterruptibly() throws InterruptedException {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                while(!canGrantWriteAccess(requestor)) {
                    mutex.wait();
                }
                writeAccesses++;
                writer = requestor;
            }
        }

        @Override
        public boolean tryLock() {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                if(canGrantWriteAccess(requestor)) {
                    writeAccesses++;
                    writer = requestor;
                    return true;
                } else {
                    return false;
                }
            }
        }

        @Override
        public boolean tryLock(long l, TimeUnit timeUnit) throws InterruptedException {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                if(canGrantWriteAccess(requestor)) {
                    writeAccesses++;
                    writer = requestor;
                    return true;
                }
                mutex.wait(timeUnit.toMillis(l),0);
                if(canGrantWriteAccess(requestor)) {
                    writeAccesses++;
                    writer = requestor;
                    return true;
                } else {
                    return false;
                }
            }
        }

        @Override
        public void unlock() {
            Object requestor = icc.getInvocationContext().getLockOwner();
            synchronized (mutex) {
                if(!writer.equals(requestor)) {
                    return ;// throw new IllegalMonitorStateException() //pshiiuuu!!
                }
                if(--writeAccesses == 0) {
                    writer = null;
                }
                mutex.notifyAll();
            }
        }

        @Override
        public Condition newCondition() {
            throw new UnsupportedOperationException();
        }
    };

    public OwnableReentrantReadWriteLock(InvocationContextContainer icc) {
        readers = new HashMap<Object, Integer>();
        writer = null;
        writeAccesses = 0;
        this.icc = icc;
    }


    //------------ readers functions -------------------//
    private boolean canGrantReadAccess(Object req) {
        if(writer == null) {
            return true;
        } else if(writer.equals(req)) {
            return true;
        } else if(readers.containsKey(req)) {
            return true;
        }
        return false;
    }

    private int getReadAccessCount(Object req) {
        Integer i = readers.get(req);
        return i != null ? i : 0;
    }


    //-------------- writer functions ------------------ //
    private boolean canGrantWriteAccess(Object req) {
        if(readers.size() == 1 && readers.containsKey(req)) {
            return true;
        } else if(readers.isEmpty()) {
            return true;
        } else if(writer == null) {
            return true;
        } else if(writer.equals(req)) {
            return true;
        }
        return false;
    }

    @Override
    public Lock readLock() {
        return readLock;
    }

    @Override
    public Lock writeLock() {
        return writeLock;
    }

    public boolean isReadOrWriteLocked() {
        return writer != null || !readers.isEmpty();
    }

    public boolean isWriteLock() {
        return writer != null;
    }

    public Object getOwner() {
        return writer;
    }

    @Override
    public String toString() {
        return new StringBuilder("OwnableReentrantReadWriteLock{")
                .append("readers=").append(readers.keySet())
                .append("writer=").append(writer)
                .append("}").toString();
    }
}
