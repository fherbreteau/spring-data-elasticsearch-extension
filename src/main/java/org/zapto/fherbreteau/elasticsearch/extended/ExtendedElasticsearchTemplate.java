package org.zapto.fherbreteau.elasticsearch.extended;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;

import java.time.Duration;

public class ExtendedElasticsearchTemplate extends ElasticsearchTemplate implements ExtendedElasticsearchOperations {

    public ExtendedElasticsearchTemplate(ElasticsearchClient client, ElasticsearchConverter elasticsearchConverter) {
        super(client, elasticsearchConverter);
    }

    @Override
    public <T> SearchHitsIterator<T> searchForStream(Query query, int fromIndex, Class<T> clazz) {
        return searchForStream(query, fromIndex, clazz, getIndexCoordinatesFor(clazz));
    }

    @Override
    public <T> SearchHitsIterator<T> searchForStream(Query query, int fromIndex, Class<T> clazz, IndexCoordinates index) {

        Duration scrollTime = query.getScrollTime() != null ? query.getScrollTime() : Duration.ofMinutes(1);
        long scrollTimeInMillis = scrollTime.toMillis();
        // noinspection ConstantConditions
        int maxCount = query.isLimiting() ? query.getMaxResults() : 0;

        return ExtendedStreamQueries.streamResults( //
                maxCount, //
                fromIndex, //
                searchScrollStart(scrollTimeInMillis, query, clazz, index), //
                scrollId -> searchScrollContinue(scrollId, scrollTimeInMillis, clazz, index), //
                this::searchScrollClear);
    }
}
