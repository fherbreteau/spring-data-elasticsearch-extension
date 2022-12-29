package org.springframework.data.elasticsearch.junit.jupiter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.client.elc.ExtendedSearchConfiguration;
import org.springframework.data.elasticsearch.core.RefreshPolicy;

import java.time.Duration;

import static org.springframework.util.StringUtils.hasText;

@Configuration
public class ElasticsearchTemplateConfiguration extends ElasticsearchConfiguration implements ExtendedSearchConfiguration {

    @Autowired
    private ClusterConnectionInfo clusterConnectionInfo;

    @Override
    public ClientConfiguration clientConfiguration() {
        String elasticsearchHostPort = clusterConnectionInfo.getHost() + ':' + clusterConnectionInfo.getHttpPort();

        ClientConfiguration.TerminalClientConfigurationBuilder configurationBuilder = ClientConfiguration.builder()
                .connectedTo(elasticsearchHostPort);

        String proxy = System.getenv("DATAES_ELASTICSEARCH_PROXY");

        if (proxy != null) {
            configurationBuilder = configurationBuilder.withProxy(proxy);
        }

        if (clusterConnectionInfo.isUseSsl()) {
            configurationBuilder = ((ClientConfiguration.MaybeSecureClientConfigurationBuilder) configurationBuilder)
                    .usingSsl();
        }

        String user = System.getenv("DATAES_ELASTICSEARCH_USER");
        String password = System.getenv("DATAES_ELASTICSEARCH_PASSWORD");

        if (hasText(user) && hasText(password)) {
            configurationBuilder.withBasicAuth(user, password);
        }

        // noinspection UnnecessaryLocalVariable
        ClientConfiguration clientConfiguration = configurationBuilder //
                .withConnectTimeout(Duration.ofSeconds(20)) //
                .withSocketTimeout(Duration.ofSeconds(20)) //
                .build();

        return clientConfiguration;
    }

    @Override
    protected RefreshPolicy refreshPolicy() {
        return RefreshPolicy.IMMEDIATE;
    }
}
