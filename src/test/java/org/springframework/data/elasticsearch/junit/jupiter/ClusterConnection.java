package org.springframework.data.elasticsearch.junit.jupiter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.lang.Nullable;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import static org.springframework.util.StringUtils.hasText;

public class ClusterConnection implements ExtensionContext.Store.CloseableResource {

    private static final Log LOGGER = LogFactory.getLog(ClusterConnection.class);

    private static final String ENV_ELASTICSEARCH_HOST = "DATAES_ELASTICSEARCH_HOST";
    private static final String ENV_ELASTICSEARCH_PORT = "DATAES_ELASTICSEARCH_PORT";

    private static final String TESTCONTAINER_IMAGE_NAME = "testcontainers.image-name";
    private static final String TESTCONTAINER_IMAGE_VERSION = "testcontainers.image-version";
    private static final int ELASTICSEARCH_DEFAULT_PORT = 9200;

    private static final ThreadLocal<ClusterConnectionInfo> clusterConnectionInfoThreadLocal = new ThreadLocal<>();

    @Nullable
    private final ClusterConnectionInfo clusterConnectionInfo;

    /**
     * creates the ClusterConnection, starting a container
     */
    public ClusterConnection() {
        clusterConnectionInfo = createClusterConnectionInfo();

        if (clusterConnectionInfo != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(clusterConnectionInfo.toString());
            }
            clusterConnectionInfoThreadLocal.set(clusterConnectionInfo);
        } else {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.error("could not create ClusterConnectionInfo");
            }
        }
    }

    /**
     * @return the {@link ClusterConnectionInfo} from the ThreadLocal storage.
     */
    @Nullable
    public static ClusterConnectionInfo clusterConnectionInfo() {
        return clusterConnectionInfoThreadLocal.get();
    }

    @Nullable
    public ClusterConnectionInfo getClusterConnectionInfo() {
        return clusterConnectionInfo;
    }

    @Nullable
    private ClusterConnectionInfo createClusterConnectionInfo() {

        String host = System.getenv(ENV_ELASTICSEARCH_HOST);

        if (hasText(host)) {

            String envPort = System.getenv(ENV_ELASTICSEARCH_PORT);

            int port = 9200;

            try {
                if (hasText(envPort)) {
                    port = Integer.parseInt(envPort);
                }
            } catch (NumberFormatException e) {
                LOGGER.warn("DATAES_ELASTICSEARCH_PORT does not contain a number");
            }

            return ClusterConnectionInfo.builder().withHostAndPort(host, port).build();
        }

        return startElasticsearchContainer();
    }

    @Nullable
    private ClusterConnectionInfo startElasticsearchContainer() {

        LOGGER.info("Starting Elasticsearch Container...");

        try {
            Map<String, String> testcontainersProperties = testcontainersProperties(
            );

            DockerImageName dockerImageName = getDockerImageName(testcontainersProperties);

            ElasticsearchContainer elasticsearchContainer = new SpringDataElasticsearchContainer(dockerImageName)
                    .withEnv(testcontainersProperties)
                    .withStartupTimeout(Duration.ofMinutes(2));
            elasticsearchContainer.start();

            return ClusterConnectionInfo.builder() //
                    .withHostAndPort(elasticsearchContainer.getHost(),
                            elasticsearchContainer.getMappedPort(ELASTICSEARCH_DEFAULT_PORT)) //
                    .withElasticsearchContainer(elasticsearchContainer) //
                    .build();
        } catch (Exception e) {
            LOGGER.error("Could not start Elasticsearch container", e);
        }

        return null;
    }

    private DockerImageName getDockerImageName(Map<String, String> testcontainersProperties) {

        String imageName = testcontainersProperties.get(TESTCONTAINER_IMAGE_NAME);
        String imageVersion = testcontainersProperties.get(TESTCONTAINER_IMAGE_VERSION);

        if (imageName == null) {
            throw new IllegalArgumentException("property " + TESTCONTAINER_IMAGE_NAME + " not configured");
        }
        testcontainersProperties.remove(TESTCONTAINER_IMAGE_NAME);

        if (imageVersion == null) {
            throw new IllegalArgumentException("property " + TESTCONTAINER_IMAGE_VERSION + " not configured");
        }
        testcontainersProperties.remove(TESTCONTAINER_IMAGE_VERSION);

        String configuredImageName = imageName + ':' + imageVersion;
        DockerImageName dockerImageName = DockerImageName.parse(configuredImageName)
                .asCompatibleSubstituteFor("docker.elastic.co/elasticsearch/elasticsearch");
        LOGGER.info("Docker image: " + dockerImageName);
        return dockerImageName;
    }

    private Map<String, String> testcontainersProperties() {

        LOGGER.info("load configuration from testcontainers.properties");

        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream("testcontainers.properties")) {
            Properties props = new Properties();

            if (inputStream != null) {
                props.load(inputStream);
            }
            Map<String, String> elasticsearchProperties = new LinkedHashMap<>();
            props.forEach((key, value) -> elasticsearchProperties.put(key.toString(), value.toString()));
            return elasticsearchProperties;
        } catch (Exception e) {
            LOGGER.error("Cannot load testcontainers.properties");
        }
        return Collections.emptyMap();
    }

    @Override
    public void close() {

        if (clusterConnectionInfo != null && clusterConnectionInfo.getElasticsearchContainer() != null) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Stopping container");
            }
            clusterConnectionInfo.getElasticsearchContainer().stop();
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("closed");
        }
    }

    private static class SpringDataElasticsearchContainer extends ElasticsearchContainer {

        public SpringDataElasticsearchContainer(DockerImageName dockerImageName) {
            super(dockerImageName);
        }

        /*
         * don't need that fancy docker whale in the logger name, this makes configuration of the log level impossible
         */
        @Override
        protected Logger logger() {
            return LoggerFactory.getLogger(SpringDataElasticsearchContainer.class);
        }
    }}
