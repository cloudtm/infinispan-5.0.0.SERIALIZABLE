package org.infinispan.api.mvcc.repeatable_read;

import org.infinispan.Cache;
import org.infinispan.CacheException;
import org.infinispan.atomic.AtomicHashMapConcurrencyTest;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheContainer;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.IsolationLevel;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.transaction.TransactionManager;
import java.util.concurrent.CountDownLatch;

/**
 * @author pedro
 *         Date: 21-06-2011
 */
@Test(groups = "unit", testName = "api.mvcc.repeatable_read.WriteSkewTest")
public class MyWriteSkewTest extends AbstractInfinispanTest {
    private static final Log log = LogFactory.getLog(AtomicHashMapConcurrencyTest.class);

    public static final String KEY = "key";
    Cache<String, Object> cache;
    TransactionManager tm;
    private CacheContainer cm;

    @BeforeMethod
    @SuppressWarnings("unchecked")
    protected void setUp() {
        Configuration c = new Configuration();
        c.setLockAcquisitionTimeout(500);
        // these 2 need to be set to use the AtomicMapCache
        c.setInvocationBatchingEnabled(true);
        c.setIsolationLevel(IsolationLevel.REPEATABLE_READ);
        c.setWriteSkewCheck(true);
        cm = TestCacheManagerFactory.createCacheManager(c, true);
        cache = cm.getCache();
        tm = TestingUtil.getTransactionManager(cache);
        cache.put(KEY, new ValueObject()); //put the value
    }

     @AfterTest
   public void tearDown() {
      TestingUtil.killCacheManagers(cm);
      tm = null;
      cache =null;
   }

    public void testWriteSkew() throws Exception {
        ValueObject val = (ValueObject) cache.get(KEY);

        assert val.val1 == 0;
        assert val.val2 == 0;
        assert val.val3 == 0;

        CountDownLatch barrier1 = new CountDownLatch(1);
        CountDownLatch barrier2 = new CountDownLatch(1);
        CountDownLatch barrier3 = new CountDownLatch(1);
        CountDownLatch barrier4 = new CountDownLatch(1);

        Tx1 tx1 = new Tx1();
        Tx2 tx2 = new Tx2();

        tx1.barrier1 = barrier1;
        tx1.barrier2 = barrier2;
        tx1.barrier3 = barrier3;
        tx1.barrier4 = barrier4;

        tx2.barrier1 = barrier1;
        tx2.barrier2 = barrier2;
        tx2.barrier3 = barrier3;
        tx2.barrier4 = barrier4;

        Thread t1 = fork(tx1, false);
        Thread t2 = fork(tx2, false);

        t1.join();
        t2.join();

        val = (ValueObject) cache.get(KEY);

        assert val.val1 == 1;
        assert val.val2 == 2;
        assert val.val3 == 3;
    }

    private static class ValueObject {
        public int val1 = 0;
        public int val2 = 0;
        public int val3 = 0;

        public String toString() {
            return "val1=" + val1 + ",val2=" + val2 + ",val3=" + val3;
        }
    }

    private class Tx1 implements Runnable {
        public CountDownLatch barrier1;
        public CountDownLatch barrier2;
        public CountDownLatch barrier3;
        public CountDownLatch barrier4;

        @Override
        public void run() {
            boolean success = true;
            try {
                tm.begin();
                ValueObject val = (ValueObject) cache.get(KEY);

                assert val.val1 == 0 : "the value1 must be 0, but it is " + val.val1;
                assert val.val2 == 0 : "the value2 must be 0, but it is " + val.val2;
                assert val.val3 == 0 : "the value3 must be 0, but it is " + val.val3;

                barrier1.await(); //wait to other read

                val.val1 = 1;
                val.val2 = 2;
                val.val3 = 3;

                barrier2.countDown(); //notify other thread
                barrier3.await(); //wait to signal and commit

                cache.put(KEY, val);

                assert val.val1 == 1 : "the value1 must be 1, but it is " + val.val1;
                assert val.val2 == 2 : "the value2 must be 2, but it is " + val.val2;
                assert val.val3 == 3 : "the value3 must be 3, but it is " + val.val3;

            } catch (Exception e) {
                success = false;
            } finally {
                try {
                    if(success) {
                        tm.commit();
                    } else {
                        tm.rollback();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
                barrier4.countDown(); //let the other try to commit
                barrier1.countDown();
                barrier2.countDown();
                barrier3.countDown();
            }
        }
    }

    private class Tx2 implements Runnable {
        public CountDownLatch barrier1;
        public CountDownLatch barrier2;
        public CountDownLatch barrier3;
        public CountDownLatch barrier4;

        @Override
        public void run() {
            boolean success = true;
            try {
                tm.begin();
                ValueObject val = (ValueObject) cache.get(KEY);

                assert val.val1 == 0 : "the value1 must be 0, but it is " + val.val1;
                assert val.val2 == 0 : "the value2 must be 0, but it is " + val.val2;
                assert val.val3 == 0 : "the value3 must be 0, but it is " + val.val3;

                barrier1.countDown(); //I read...
                barrier2.await(); //the other will change and no put it in the cache

                assert val.val1 == 0 : "the value1 was 0 when I read the first time and now is " + val.val1;
                assert val.val2 == 0 : "the value1 was 0 when I read the first time and now is " + val.val2;
                assert val.val3 == 0 : "the value1 was 0 when I read the first time and now is " + val.val3;

                barrier3.countDown(); //lets the other tx commit
                barrier4.await(); //the other tx commits... we must throw a write skew check

                assert val.val1 == 0 : "the value1 was 0 when I read the first time and now is " + val.val1;
                assert val.val2 == 0 : "the value1 was 0 when I read the first time and now is " + val.val2;
                assert val.val3 == 0 : "the value1 was 0 when I read the first time and now is " + val.val3;

                val.val1 = 4;
                val.val1 = 5;
                val.val1 = 6;

                cache.put(KEY, val); //write skew throw

                assert false : "we must throw a write skew check exception";

            } catch (Exception e) {
                assert e instanceof CacheException;
                success = false;
            } finally {
                try {
                    if(success) {
                        tm.commit();
                    } else {
                        tm.rollback();
                    }
                } catch (Exception e) {
                }
                barrier1.countDown();
                barrier2.countDown();
                barrier3.countDown();
                barrier4.countDown();
            }
        }
    }
}
