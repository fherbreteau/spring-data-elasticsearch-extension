package org.zapto.fherbreteau.elasticsearch.extended;

import org.springframework.data.elasticsearch.core.ElasticsearchOperations;

public interface ExtendedElasticsearchOperations extends ElasticsearchOperations, ExtendedSearchOperations {

}
