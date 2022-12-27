package org.springframework.data.elasticsearch.client.elc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.elasticsearch.core.ExtendedSearchOperations;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;

@Configuration
public interface ExtendedSearchConfiguration {

    /**
     * Creates {@link ExtendedSearchOperations}.
     *
     * @return never {@literal null}.
     */
    @Bean(name = {"extendedSearchOperations", "extendedSearchTemplate"})
    default ExtendedSearchOperations extendedSearchOperations(ElasticsearchConverter elasticsearchConverter,
                                                              ElasticsearchClient elasticsearchClient) {

        return new ExtendedElasticsearchTemplate(elasticsearchClient, elasticsearchConverter);
    }
}
