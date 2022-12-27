package org.springframework.data.elasticsearch.core;

import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;

import javax.annotation.Nullable;

public interface ExtendedSearchOperations {

    IndexCoordinates getIndexCoordinatesFor(Class<?> clazz);

    default <T> SearchHitsIterator<T> searchForStream(@Nullable Query query, int fromIndex, Class<T> clazz) {
        return searchForStream(query, fromIndex, clazz, getIndexCoordinatesFor(clazz));
    }

    <T> SearchHitsIterator<T> searchForStream(@Nullable Query query, int fromIndex, Class<T> clazz, IndexCoordinates index);
}
