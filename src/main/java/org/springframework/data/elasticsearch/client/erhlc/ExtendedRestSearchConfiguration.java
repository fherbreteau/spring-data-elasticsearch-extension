package org.springframework.data.elasticsearch.client.erhlc;

import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.context.annotation.Bean;
import org.springframework.data.elasticsearch.core.ExtendedSearchOperations;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;

/**
 * @since 0.1
 * @deprecated since 1.0
 */
@Deprecated(since = "1.0")
public interface ExtendedRestSearchConfiguration {

    /**
     * Creates {@link ExtendedSearchOperations} implementation using a {@link org.elasticsearch.client.RestHighLevelClient}.
     *
     * @return never {@literal null}.
     */
    @Bean(name = { "extendedSearchOperations", "extendedSearchTemplate" })
    default ExtendedSearchOperations extendedSearchOperations(ElasticsearchConverter elasticsearchConverter,
                                                             RestHighLevelClient elasticsearchClient) {

        return new ExtendedElasticsearchRestTemplate(elasticsearchClient, elasticsearchConverter);
    }
}
