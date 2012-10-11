package org.infinispan.factories;



import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.mvcc.VersionVCFactory;


/**
 * Factory for building VersionVCFactory object
 *
 *
 * @author <a href="mailto:peluso@gsd.inesc-id.pt">Sebastiano Peluso</a>
 * @since 5.0
 */
@DefaultFactoryFor(classes = VersionVCFactory.class)
public class VersionVCFactoryFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {
	@SuppressWarnings("unchecked")
	   public <T> T construct(Class<T> componentType) {
	      if (configuration.getCacheMode().isDistributed())
	         return (T) new VersionVCFactory(true);
	      else
	         return (T) new VersionVCFactory(false);
	   }
	

}
