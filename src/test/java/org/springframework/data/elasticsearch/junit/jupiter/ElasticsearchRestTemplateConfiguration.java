package org.springframework.data.elasticsearch.junit.jupiter;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.data.elasticsearch.client.ClientConfiguration;
import org.springframework.data.elasticsearch.client.erhlc.AbstractElasticsearchConfiguration;
import org.springframework.data.elasticsearch.client.erhlc.ElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.client.erhlc.ExtendedRestSearchConfiguration;
import org.springframework.data.elasticsearch.client.erhlc.RestClients;
import org.springframework.data.elasticsearch.core.ElasticsearchOperations;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.support.HttpHeaders;

import java.time.Duration;

import static org.springframework.util.StringUtils.hasText;

@Deprecated
@Configuration
public class ElasticsearchRestTemplateConfiguration extends AbstractElasticsearchConfiguration implements ExtendedRestSearchConfiguration {
    @Autowired
    private ClusterConnectionInfo clusterConnectionInfo;

    @Override
    @Bean
    public RestHighLevelClient elasticsearchClient() {

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
    public ElasticsearchOperations elasticsearchOperations(ElasticsearchConverter elasticsearchConverter,
                                                           RestHighLevelClient elasticsearchClient) {

        ElasticsearchRestTemplate template = new ElasticsearchRestTemplate(elasticsearchClient, elasticsearchConverter) {
            @Override
            public <T> T execute(ClientCallback<T> callback) {
                try {
                    return super.execute(callback);
                } catch (DataAccessResourceFailureException e) {
                    try {
                        Thread.sleep(1_000);
                    } catch (InterruptedException ignored) {}
                    return super.execute(callback);
                }
            }
        };
        template.setRefreshPolicy(refreshPolicy());

        return template;
    }

    @Override
    protected RefreshPolicy refreshPolicy() {
        return RefreshPolicy.IMMEDIATE;
    }
}
