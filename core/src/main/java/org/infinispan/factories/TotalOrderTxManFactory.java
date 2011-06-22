package org.infinispan.factories;

import org.infinispan.config.ConfigurationException;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.totalorder.TotalOrderTransactionManager;

/**
 * @author pedro
 *         Date: 16-06-2011
 */
@DefaultFactoryFor(classes = {TotalOrderTransactionManager.class})
public class TotalOrderTxManFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

    @Override
    public <T> T construct(Class<T> componentType) {
        if(componentType != TotalOrderTransactionManager.class) {
            throw new ConfigurationException("Don't know how to construct " + componentType);
        }
        return (T) new TotalOrderTransactionManager(configuration.getConcurrencyLevel());

    }
}
