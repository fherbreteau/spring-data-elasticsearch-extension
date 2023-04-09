package org.springframework.data.elasticsearch.junit.jupiter;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.erhlc.AbstractElasticsearchConfiguration;
import org.springframework.data.elasticsearch.client.erhlc.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.client.erhlc.ExtendedRestSearchConfiguration;
import org.springframework.data.elasticsearch.client.erhlc.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.support.HttpHeaders;

import javax.annotation.Nonnull;
import java.time.Duration;

import static org.springframework.util.StringUtils.hasText;

@Deprecated(since = "1.0")
@Configuration
public class ElasticsearchRestTemplateConfiguration extends AbstractElasticsearchConfiguration implements ExtendedRestSearchConfiguration {
    @Autowired
    private ClusterConnectionInfo clusterConnectionInfo;

    @Override
    @Bean
    public @Nonnull RestHighLevelClient elasticsearchClient() {

        String elasticsearchHostPort = clusterConnectionInfo.getHost() + ':' + clusterConnectionInfo.getHttpPort();

        ClientConfiguration.TerminalClientConfigurationBuilder configurationBuilder = ClientConfiguration.builder()
                .connectedTo(elasticsearchHostPort);

        if (clusterConnectionInfo.isUseSsl()) {
            configurationBuilder = ((ClientConfiguration.MaybeSecureClientConfigurationBuilder) configurationBuilder)
                    .usingSsl();
        }

        HttpHeaders defaultHeaders = new HttpHeaders();
        defaultHeaders.add("Accept", "application/vnd.elasticsearch+json;compatible-with=7");
        defaultHeaders.add("Content-Type", "application/vnd.elasticsearch+json;compatible-with=7");

        // noinspection resource
        return RestClients.create(configurationBuilder //
                        .withDefaultHeaders(defaultHeaders) //
                        .withConnectTimeout(Duration.ofSeconds(20)) //
                        .withSocketTimeout(Duration.ofSeconds(20)) //
                        .build()) //
                .rest();
    }

    @Override
    public @Nonnull ElasticsearchOperations elasticsearchOperations(@Nonnull ElasticsearchConverter elasticsearchConverter,
                                                                    @Nonnull RestHighLevelClient elasticsearchClient) {

        ElasticsearchRestTemplate template = new ElasticsearchRestTemplate(elasticsearchClient, elasticsearchConverter);
        template.setRefreshPolicy(refreshPolicy());

        return template;
    }

    @Override
    protected RefreshPolicy refreshPolicy() {
        return RefreshPolicy.IMMEDIATE;
    }
}
