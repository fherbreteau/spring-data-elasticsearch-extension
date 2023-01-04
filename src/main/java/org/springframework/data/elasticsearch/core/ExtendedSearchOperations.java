package org.springframework.data.elasticsearch.core;

import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface ExtendedSearchOperations {

    IndexCoordinates getIndexCoordinatesFor(Class<?> clazz);

    @Nonnull
    default <T> SearchHitsIterator<T> searchForStream(@Nullable Query query, int fromIndex, Class<T> clazz) {
        return searchForStream(query, fromIndex, clazz, getIndexCoordinatesFor(clazz));
    }

    @Nonnull
    <T> SearchHitsIterator<T> searchForStream(@Nullable Query query, int fromIndex, Class<T> clazz, IndexCoordinates index);
}
