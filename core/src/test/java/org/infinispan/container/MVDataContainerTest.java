package org.infinispan.container;

/**
 * @author pedro
 *         Date: 04-08-2011
 */
public class MVDataContainerTest extends SimpleDataContainerTest {
    @Override
    protected DataContainer createContainer() {
        return new MultiVersionDataContainer(16);
    }
}
