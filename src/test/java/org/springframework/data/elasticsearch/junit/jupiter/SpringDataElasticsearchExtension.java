package org.springframework.data.elasticsearch.junit.jupiter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.extension.*;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.test.context.ContextConfigurationAttributes;
import org.springframework.test.context.ContextCustomizer;
import org.springframework.test.context.ContextCustomizerFactory;
import org.springframework.test.context.MergedContextConfiguration;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class SpringDataElasticsearchExtension
        implements BeforeAllCallback, ParameterResolver, ContextCustomizerFactory {

    private static final Log LOGGER = LogFactory.getLog(SpringDataElasticsearchExtension.class);

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
            .create(SpringDataElasticsearchExtension.class.getName());
    private static final String STORE_KEY_CLUSTER_CONNECTION = ClusterConnection.class.getSimpleName();
    private static final String STORE_KEY_CLUSTER_CONNECTION_INFO = ClusterConnectionInfo.class.getSimpleName();

    private static final Lock initLock = new ReentrantLock();

    @Override
    public void beforeAll(ExtensionContext extensionContext) {
        initLock.lock();
        try {
            ExtensionContext.Store store = getStore(extensionContext);
            ClusterConnection clusterConnection = store.getOrComputeIfAbsent(STORE_KEY_CLUSTER_CONNECTION, key -> {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("creating ClusterConnection");
                }
                return createClusterConnection();
            }, ClusterConnection.class);
            store.getOrComputeIfAbsent(STORE_KEY_CLUSTER_CONNECTION_INFO,
                    key -> clusterConnection.getClusterConnectionInfo());
        } finally {
            initLock.unlock();
        }
    }

    private ExtensionContext.Store getStore(ExtensionContext extensionContext) {
        return extensionContext.getRoot().getStore(NAMESPACE);
    }

    private ClusterConnection createClusterConnection() {
        return new ClusterConnection();
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        Class<?> parameterType = parameterContext.getParameter().getType();
        return parameterType.isAssignableFrom(ClusterConnectionInfo.class);
    }

    /*
     * (non javadoc)
     * no need to check the parameterContext and extensionContext here, this was done before in supportsParameter.
     */
    @Override
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext)
            throws ParameterResolutionException {
        return getStore(extensionContext).get(STORE_KEY_CLUSTER_CONNECTION_INFO, ClusterConnectionInfo.class);
    }

    @Override
    public ContextCustomizer createContextCustomizer(@Nonnull Class<?> testClass,
                                                     @Nonnull List<ContextConfigurationAttributes> configAttributes) {
        return this::customizeContext;
    }

    private void customizeContext(ConfigurableApplicationContext context, MergedContextConfiguration mergedConfig) {

        ClusterConnectionInfo clusterConnectionInfo = ClusterConnection.clusterConnectionInfo();

        if (clusterConnectionInfo != null) {
            context.getBeanFactory().registerResolvableDependency(ClusterConnectionInfo.class, clusterConnectionInfo);
        }
    }
}
