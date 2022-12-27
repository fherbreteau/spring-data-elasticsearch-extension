package org.springframework.data.elasticsearch.config;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ExtendedElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.core.ExtendedSearchOperations;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;

@Configuration
public interface ExtendedRestSearchConfiguration {

    /**
     * Creates {@link ExtendedSearchOperations}.
     *
     * @return never {@literal null}.
     */
    @Bean(name = { "extendedSearchOperations", "extendedSearchTemplate" })
    default ExtendedSearchOperations extendedSearchOperations(ElasticsearchConverter elasticsearchConverter,
                                                             RestHighLevelClient elasticsearchClient) {

        return new ExtendedElasticsearchRestTemplate(elasticsearchClient, elasticsearchConverter);
    }
}
