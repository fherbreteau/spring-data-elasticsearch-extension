package org.springframework.data.elasticsearch.client.elc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.core.ExtendedSearchOperations;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;

public interface ExtendedSearchConfiguration {

    /**
     * Creates a {@link ExtendedSearchOperations} implementation using an
     * {@link co.elastic.clients.elasticsearch.ElasticsearchClient}.
     *
     * @return never {@literal null}.
     */
    @Bean(name = {"extendedSearchOperations", "extendedSearchTemplate"})
    default ExtendedSearchOperations extendedSearchOperations(ElasticsearchConverter elasticsearchConverter,
                                                              ElasticsearchClient elasticsearchClient) {

        return new ExtendedElasticsearchTemplate(elasticsearchClient, elasticsearchConverter);
    }
}
