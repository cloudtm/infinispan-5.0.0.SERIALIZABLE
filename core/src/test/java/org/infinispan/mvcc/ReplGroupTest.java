package org.infinispan.mvcc;

import org.infinispan.config.Configuration;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.notifications.cachelistener.CacheNotifier;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterTest;
import org.testng.annotations.Test;

import java.util.List;

/**
 * @author pedro
 *         Date: 08-08-2011
 */
@Test(groups = "functional", testName = "mvcc.ReplGroupTest")
@CleanupAfterTest
public class ReplGroupTest extends MultipleCacheManagersTest {
    Configuration config = new Configuration();

    @Override
    protected void createCacheManagers() throws Throwable {
        config.setCacheMode(Configuration.CacheMode.DIST_SYNC);
        config.setNumOwners(2);
        createCluster(config, 3);
        waitForClusterToForm();
        cache(0);
        cache(1);
        cache(2);
    }

    private void assertLocation(DistributionManager dm, int numOfKeys) {
        for(int i = 0; i < numOfKeys; ++i) {
            ReplGroup g = dm.locateGroup("key" + i);
            List<Address> d = dm.locate("key" + i);
            assert g != null : "The group is null for key " + i;
            assert d.containsAll(g.getMembers()) : "the members are different " + g + " vs " + d + " for key " + i;
        }
    }

    public void testLocation() {
        int numOfKey = 1000;

        for(int i = 0; i < numOfKey; ++i) {
            cache(0).put("key" + i, "value" + i);
        }

        DistributionManager dm = cache(0).getAdvancedCache().getDistributionManager();

        assertLocation(dm, numOfKey);
    }

    public void testFailedMember() {
        DistributionManager dm = cache(0).getAdvancedCache().getDistributionManager();
        int numOfKey = 1000;

        for(int i = 0; i < numOfKey; ++i) {
            cache(0).put("key" + i, "value" + i);
        }

        assertLocation(dm, numOfKey);

        killMember(2);

        assertLocation(dm, numOfKey);
    }

    public void testAddMember() {
        DistributionManager dm = cache(0).getAdvancedCache().getDistributionManager();
        int numOfKey = 1000;

        for(int i = 0; i < numOfKey; ++i) {
            cache(0).put("key" + i, "value" + i);
        }

        assertLocation(dm, numOfKey);

        addClusterEnabledCacheManager(config);

        assertLocation(dm, numOfKey);
    }
}
