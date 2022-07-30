package org.zapto.fherbreteau.elasticsearch.extended;

import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.SearchOperations;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;

public interface ExtendedSearchOperations extends SearchOperations {

    <T> SearchHitsIterator<T> searchForStream(Query query, int fromIndex, Class<T> clazz);

    <T> SearchHitsIterator<T> searchForStream(Query query, int fromIndex, Class<T> clazz, IndexCoordinates index);
}
