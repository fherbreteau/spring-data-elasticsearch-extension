package org.springframework.data.elasticsearch.junit.jupiter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchConfiguration;
import org.springframework.data.elasticsearch.client.elc.ExtendedSearchConfiguration;
import org.springframework.data.elasticsearch.core.RefreshPolicy;

import java.time.Duration;

@Configuration
public class ElasticsearchTemplateConfiguration extends ElasticsearchConfiguration implements ExtendedSearchConfiguration {

    @Autowired
    private ClusterConnectionInfo clusterConnectionInfo;

    @Override
    public ClientConfiguration clientConfiguration() {
        String elasticsearchHostPort = clusterConnectionInfo.getHost() + ':' + clusterConnectionInfo.getHttpPort();

        ClientConfiguration.TerminalClientConfigurationBuilder configurationBuilder = ClientConfiguration.builder()
                .connectedTo(elasticsearchHostPort);

        if (clusterConnectionInfo.isUseSsl()) {
            configurationBuilder = ((ClientConfiguration.MaybeSecureClientConfigurationBuilder) configurationBuilder)
                    .usingSsl();
        }

        return configurationBuilder //
                .withConnectTimeout(Duration.ofSeconds(20)) //
                .withSocketTimeout(Duration.ofSeconds(20)) //
                .build();
    }

    @Override
    protected RefreshPolicy refreshPolicy() {
        return RefreshPolicy.IMMEDIATE;
    }
}
