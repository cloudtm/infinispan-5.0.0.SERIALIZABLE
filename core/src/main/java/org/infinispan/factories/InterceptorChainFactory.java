/*
 * JBoss, Home of Professional Open Source
 * Copyright 2009 Red Hat Inc. and/or its affiliates and other
 * contributors as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a full listing of
 * individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.infinispan.factories;


import org.infinispan.CacheException;
import org.infinispan.config.Configuration;
import org.infinispan.config.ConfigurationException;
import org.infinispan.config.CustomInterceptorConfig;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.interceptors.*;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.interceptors.SerialLockingInterceptor;
import org.infinispan.interceptors.SerialTxInterceptor;
import org.infinispan.loaders.CacheLoaderConfig;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.util.Util;
import org.infinispan.util.concurrent.IsolationLevel;

import java.util.List;

/**
 * Factory class that builds an interceptor chain based on cache configuration.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani (manik@jboss.org)</a>
 * @since 4.0
 */
@DefaultFactoryFor(classes = InterceptorChain.class)
public class InterceptorChainFactory extends AbstractNamedCacheComponentFactory implements AutoInstantiableFactory {

    public CommandInterceptor createInterceptor(Class<? extends CommandInterceptor> clazz) {
        CommandInterceptor chainedInterceptor = componentRegistry.getComponent(clazz);
        if (chainedInterceptor == null) {
            chainedInterceptor = Util.getInstance(clazz);
            try {
                componentRegistry.registerComponent(chainedInterceptor, clazz);
            }
            catch (RuntimeException e) {
                log.warn("Problems creating interceptor " + clazz);
                throw e;
            }
        } else {
            // wipe next/last chaining!!
            chainedInterceptor.setNext(null);
        }
        return chainedInterceptor;
    }

    private boolean isUsingMarshalledValues(Configuration c) {
        return c.isStoreAsBinary() && (c.isStoreKeysAsBinary() || c.isStoreValuesAsBinary());
    }

    public InterceptorChain buildInterceptorChain() throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        boolean invocationBatching = configuration.isInvocationBatchingEnabled();
        boolean serializability = configuration.getIsolationLevel() == IsolationLevel.SERIALIZABLE;
        // load the icInterceptor first

        CommandInterceptor first = invocationBatching ? createInterceptor(BatchingInterceptor.class) : createInterceptor(InvocationContextInterceptor.class);

        InterceptorChain interceptorChain = new InterceptorChain(first);

        // add the interceptor chain to the registry first, since some interceptors may ask for it.
        componentRegistry.registerComponent(interceptorChain, InterceptorChain.class);

        // add marshallable check interceptor for situations where we want to figure out before marshalling
        if (isUsingMarshalledValues(configuration) || configuration.isUseAsyncMarshalling()
                || configuration.isUseReplQueue() || hasAsyncStore())
            interceptorChain.appendInterceptor(createInterceptor(IsMarshallableInterceptor.class));

        // NOW add the ICI if we are using batching!
        if (invocationBatching)
            interceptorChain.appendInterceptor(createInterceptor(InvocationContextInterceptor.class));

        if(configuration.isTotalOrderReplication()) {
            if(serializability) {
                interceptorChain.appendInterceptor(createInterceptor(SerialTotalOrderInterceptor.class));
            } else if(configuration.getCacheMode().isReplicated()) {
                interceptorChain.appendInterceptor(createInterceptor(TotalOrderInterceptor.class));
            } else {
                interceptorChain.appendInterceptor(createInterceptor(DistTotalOrderInterceptor.class));
            }
        }

        // load the cache management interceptor next
        if (configuration.isExposeJmxStatistics())
            interceptorChain.appendInterceptor(createInterceptor(CacheMgmtInterceptor.class));

        // load the tx interceptor
        if (configuration.getCacheMode().isDistributed()) {
            if(serializability) {
                interceptorChain.appendInterceptor(createInterceptor(SerialDistTxInterceptor.class));
            } else {
                interceptorChain.appendInterceptor(createInterceptor(DistTxInterceptor.class));
            }
        } else {
            if(serializability) {
                interceptorChain.appendInterceptor(createInterceptor(SerialTxInterceptor.class));
            } else {
                interceptorChain.appendInterceptor(createInterceptor(TxInterceptor.class));
            }
        }

        if(configuration.isUseEagerLocking())
            interceptorChain.appendInterceptor(createInterceptor(ImplicitEagerLockingInterceptor.class));

        if (isUsingMarshalledValues(configuration))
            interceptorChain.appendInterceptor(createInterceptor(MarshalledValueInterceptor.class));

        interceptorChain.appendInterceptor(createInterceptor(NotificationInterceptor.class));

        if (configuration.isEnableDeadlockDetection()) {
            interceptorChain.appendInterceptor(createInterceptor(DeadlockDetectingInterceptor.class));
        }

        if (configuration.isUsingCacheLoaders()) {
            if (configuration.getCacheLoaderManagerConfig().isPassivation()) {
                interceptorChain.appendInterceptor(createInterceptor(ActivationInterceptor.class));
                interceptorChain.appendInterceptor(createInterceptor(PassivationInterceptor.class));
            } else {
                interceptorChain.appendInterceptor(createInterceptor(CacheLoaderInterceptor.class));
                switch (configuration.getCacheMode()) {
                    case DIST_SYNC:
                    case DIST_ASYNC:
                        interceptorChain.appendInterceptor(createInterceptor(DistCacheStoreInterceptor.class));
                        break;
                    default:
                        interceptorChain.appendInterceptor(createInterceptor(CacheStoreInterceptor.class));
                        break;
                }
            }
        }


        if (configuration.getCacheMode().isDistributed()) {
            if(serializability) {
                interceptorChain.appendInterceptor(createInterceptor(SerialDistLockingInterceptor.class));
            } else {
                interceptorChain.appendInterceptor(createInterceptor(DistLockingInterceptor.class));
            }
        } else {
            if(serializability) {
                interceptorChain.appendInterceptor(createInterceptor(SerialLockingInterceptor.class));
            } else {
                interceptorChain.appendInterceptor(createInterceptor(LockingInterceptor.class));
            }
        }

        switch (configuration.getCacheMode()) {
            case REPL_SYNC:
            case REPL_ASYNC:
                if(serializability) {
                    interceptorChain.appendInterceptor(createInterceptor(SerialReplicationInterceptor.class));
                } else {
                    interceptorChain.appendInterceptor(createInterceptor(ReplicationInterceptor.class));
                }
                break;
            case INVALIDATION_SYNC:
            case INVALIDATION_ASYNC:
                interceptorChain.appendInterceptor(createInterceptor(InvalidationInterceptor.class));
                break;
            case DIST_SYNC:
            case DIST_ASYNC:
                if(serializability) {
                    interceptorChain.appendInterceptor(createInterceptor(SerialDistributionInterceptor.class));
                } else {
                    interceptorChain.appendInterceptor(createInterceptor(DistributionInterceptor.class));
                }
                break;
            case LOCAL:
                //Nothing...
        }

        CommandInterceptor callInterceptor = createInterceptor(CallInterceptor.class);
        interceptorChain.appendInterceptor(callInterceptor);
        if (log.isTraceEnabled()) log.trace("Finished building default interceptor chain.");
        buildCustomInterceptors(interceptorChain, configuration.getCustomInterceptors());
        return interceptorChain;
    }

    @SuppressWarnings("unchecked")
    private Class<? extends CommandInterceptor> getCustomInterceptorType(CustomInterceptorConfig cfg) {
        if (cfg.getInterceptor() != null) return cfg.getInterceptor().getClass();
        return Util.loadClass(cfg.getClassName(), Thread.currentThread().getContextClassLoader());
    }

    private CommandInterceptor getOrCreateCustomInterceptor(CustomInterceptorConfig cfg) {
        if (cfg.getInterceptor() != null) return cfg.getInterceptor();
        return (CommandInterceptor) Util.getInstance(cfg.getClassName(), Thread.currentThread().getContextClassLoader());
    }

    private void buildCustomInterceptors(InterceptorChain interceptorChain, List<CustomInterceptorConfig> customInterceptors) {
        for (CustomInterceptorConfig config : customInterceptors) {
            if (interceptorChain.containsInterceptorType(getCustomInterceptorType(config))) continue;
            if (config.isFirst())
                interceptorChain.addInterceptor(getOrCreateCustomInterceptor(config), 0);
            else if (config.isLast())
                interceptorChain.appendInterceptor(getOrCreateCustomInterceptor(config));
            else if (config.getIndex() >= 0)
                interceptorChain.addInterceptor(getOrCreateCustomInterceptor(config), config.getIndex());
            else if (config.getAfter() != null) {
                List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getAfter());
                if (withClassName.isEmpty()) {
                    throw new ConfigurationException("Cannot add after class: " + config.getAfter()
                            + " as no such interceptor exists in the default chain");
                }
                interceptorChain.addInterceptorAfter(getOrCreateCustomInterceptor(config), withClassName.get(0).getClass());
            }
            else if (config.getBefore() != null) {
                List<CommandInterceptor> withClassName = interceptorChain.getInterceptorsWithClassName(config.getBefore());
                if (withClassName.isEmpty()) {
                    throw new ConfigurationException("Cannot add before class: " + config.getAfter()
                            + " as no such interceptor exists in the default chain");
                }
                interceptorChain.addInterceptorBefore(getOrCreateCustomInterceptor(config), withClassName.get(0).getClass());
            }
        }

    }

    private boolean hasAsyncStore() {
        List<CacheLoaderConfig> loaderConfigs = configuration.getCacheLoaderManagerConfig().getCacheLoaderConfigs();
        for (CacheLoaderConfig loaderConfig : loaderConfigs) {
            if (loaderConfig instanceof CacheStoreConfig) {
                CacheStoreConfig storeConfig = (CacheStoreConfig) loaderConfig;
                if (storeConfig.getAsyncStoreConfig().isEnabled())
                    return true;
            }
        }
        return false;
    }

    @Override
    public <T> T construct(Class<T> componentType) {
        try {
            return componentType.cast(buildInterceptorChain());
        } catch (CacheException ce) {
            throw ce;
        }
        catch (Exception e) {
            throw new ConfigurationException("Unable to build interceptor chain", e);
        }
    }

    public static InterceptorChainFactory getInstance(ComponentRegistry componentRegistry, Configuration configuration) {
        InterceptorChainFactory icf = new InterceptorChainFactory();
        icf.componentRegistry = componentRegistry;
        icf.configuration = configuration;
        return icf;
    }
}
