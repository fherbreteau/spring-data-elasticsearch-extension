package org.springframework.data.elasticsearch.client.elc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Time;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.ResponseBody;
import co.elastic.clients.json.JsonpMapper;
import org.springframework.data.elasticsearch.core.AbstractExtendedSearchTemplate;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.List;

public class ExtendedElasticsearchTemplate extends AbstractExtendedSearchTemplate {

    private final ElasticsearchClient client;
    private final JsonpMapper jsonpMapper;
    private final RequestConverter requestConverter;
    private final ElasticsearchExceptionTranslator exceptionTranslator;

    public ExtendedElasticsearchTemplate(ElasticsearchClient client) {

        Assert.notNull(client, "client must not be null");

        this.client = client;
        this.jsonpMapper = client._transport().jsonpMapper();
        requestConverter = new RequestConverter(elasticsearchConverter, jsonpMapper);
        exceptionTranslator = new ElasticsearchExceptionTranslator(jsonpMapper);
    }

    public ExtendedElasticsearchTemplate(ElasticsearchClient client, ElasticsearchConverter elasticsearchConverter) {
        super(elasticsearchConverter);

        Assert.notNull(client, "client must not be null");

        this.client = client;
        this.jsonpMapper = client._transport().jsonpMapper();
        requestConverter = new RequestConverter(elasticsearchConverter, jsonpMapper);
        exceptionTranslator = new ElasticsearchExceptionTranslator(jsonpMapper);
    }

    @Override
    public <T> SearchScrollHits<T> searchScrollStart(long scrollTimeInMillis, Query query, Class<T> clazz,
                                                     IndexCoordinates index) {

        Assert.notNull(query, "query must not be null");
        Assert.notNull(query.getPageable(), "pageable of query must not be null.");

        SearchRequest request = requestConverter.searchRequest(query, clazz, index, false, scrollTimeInMillis);
        SearchResponse<EntityAsMap> response = execute(client -> client.search(request, EntityAsMap.class));

        return getSearchScrollHits(clazz, index, response);
    }

    @Override
    public <T> SearchScrollHits<T> searchScrollContinue(String scrollId, long scrollTimeInMillis, Class<T> clazz,
                                                        IndexCoordinates index) {

        Assert.notNull(scrollId, "scrollId must not be null");

        ScrollRequest request = ScrollRequest
                .of(sr -> sr.scrollId(scrollId).scroll(Time.of(t -> t.time(scrollTimeInMillis + "ms"))));
        ScrollResponse<EntityAsMap> response = execute(client -> client.scroll(request, EntityAsMap.class));

        return getSearchScrollHits(clazz, index, response);
    }

    private <T> SearchScrollHits<T> getSearchScrollHits(Class<T> clazz, IndexCoordinates index,
                                                        ResponseBody<EntityAsMap> response) {
        ReadDocumentCallback<T> documentCallback = new ReadDocumentCallback<>(elasticsearchConverter, clazz, index);
        SearchDocumentResponseCallback<SearchScrollHits<T>> callback = new ReadSearchScrollDocumentResponseCallback<>(clazz,
                index);

        return callback.doWith(SearchDocumentResponseBuilder.from(response, getEntityCreator(documentCallback), jsonpMapper));
    }

    @Override
    public void searchScrollClear(List<String> scrollIds) {
        Assert.notNull(scrollIds, "scrollIds must not be null");

        if (!scrollIds.isEmpty()) {
            ClearScrollRequest request = ClearScrollRequest.of(csr -> csr.scrollId(scrollIds));
            execute(client -> client.clearScroll(request));
        }
    }

    /**
     * Callback interface to be used with {@link #execute(ClientCallback)} for operating directly on
     * the {@link ElasticsearchClient}.
     */
    @FunctionalInterface
    public interface ClientCallback<T> {
        T doWithClient(ElasticsearchClient client) throws IOException;
    }

    /**
     * Execute a callback with the {@link ElasticsearchClient} and provide exception translation.
     *
     * @param callback the callback to execute, must not be {@literal null}
     * @param <T> the type returned from the callback
     * @return the callback result
     */
    public <T> T execute(ClientCallback<T> callback) {

        Assert.notNull(callback, "callback must not be null");

        try {
            return callback.doWithClient(client);
        } catch (IOException | RuntimeException e) {
            throw exceptionTranslator.translateException(e);
        }
    }
}
