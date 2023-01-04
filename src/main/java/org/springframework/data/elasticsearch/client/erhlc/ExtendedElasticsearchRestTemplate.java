package org.springframework.data.elasticsearch.client.erhlc;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.core.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.core.AbstractExtendedSearchTemplate;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.util.Assert;

import java.io.IOException;
import java.util.List;

/**
 * ElasticsearchRestTemplate
 * @since 0.1
 * @deprecated since 1.0
 */
@Deprecated(since = "1.0")
public class ExtendedElasticsearchRestTemplate extends AbstractExtendedSearchTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExtendedElasticsearchRestTemplate.class);

    private final RestHighLevelClient client;
    private final ElasticsearchExceptionTranslator exceptionTranslator = new ElasticsearchExceptionTranslator();
    protected RequestFactory requestFactory;

    public ExtendedElasticsearchRestTemplate(RestHighLevelClient client) {

        super();

        Assert.notNull(client, "client must not be null");

        this.client = client;
        requestFactory = new RequestFactory(this.elasticsearchConverter);
    }

    public ExtendedElasticsearchRestTemplate(RestHighLevelClient client, ElasticsearchConverter elasticsearchConverter) {

        super(elasticsearchConverter);

        Assert.notNull(client, "client must not be null");

        this.client = client;
        requestFactory = new RequestFactory(this.elasticsearchConverter);

    }

    @Override
    public <T> SearchScrollHits<T> searchScrollStart(long scrollTimeInMillis, Query query, Class<T> clazz,
                                                        IndexCoordinates index) {

        Assert.notNull(query.getPageable(), "pageable of query must not be null.");

        SearchRequest request = requestFactory.searchRequest(query, clazz, index);
        request.scroll(TimeValue.timeValueMillis(scrollTimeInMillis));

        SearchResponse response = execute(client -> client.search(request, RequestOptions.DEFAULT));

        return getSearchScrollHits(clazz, index, response);
    }

    @Override
    public <T> SearchScrollHits<T> searchScrollContinue(String scrollId, long scrollTimeInMillis, Class<T> clazz, IndexCoordinates index) {

        SearchScrollRequest request = new SearchScrollRequest(scrollId);
        request.scroll(TimeValue.timeValueMillis(scrollTimeInMillis));

        SearchResponse response = execute(client -> client.scroll(request, RequestOptions.DEFAULT));

        return getSearchScrollHits(clazz, index, response);
    }

    private <T> SearchScrollHits<T> getSearchScrollHits(Class<T> clazz, IndexCoordinates index,
                                                        SearchResponse response) {

        ReadDocumentCallback<T> documentCallback = new ReadDocumentCallback<>(elasticsearchConverter, clazz, index);
        SearchDocumentResponseCallback<SearchScrollHits<T>> callback = new ReadSearchScrollDocumentResponseCallback<>(clazz,
                index);
        return callback.doWith(SearchDocumentResponseBuilder.from(response, getEntityCreator(documentCallback)));
    }

    @Override
    public void searchScrollClear(List<String> scrollIds) {
        try {
            ClearScrollRequest request = new ClearScrollRequest();
            request.scrollIds(scrollIds);
            execute(client -> client.clearScroll(request, RequestOptions.DEFAULT));
        } catch (Exception e) {
            LOGGER.warn(String.format("Could not clear scroll: %s", e.getMessage()));
        }
    }

    /**
     * Callback interface to be used with {@link #execute(ClientCallback)} for operating directly on
     * {@link RestHighLevelClient}.
     *
     * @since 4.0
     */
    @FunctionalInterface
    public interface ClientCallback<T> {
        T doWithClient(RestHighLevelClient client) throws IOException;
    }

    /**
     * Execute a callback with the {@link RestHighLevelClient}
     *
     * @param callback the callback to execute, must not be {@literal null}
     * @param <T> the type returned from the callback
     * @return the callback result
     * @since 4.0
     */
    public <T> T execute(ClientCallback<T> callback) {

        Assert.notNull(callback, "callback must not be null");

        try {
            return callback.doWithClient(client);
        } catch (IOException | RuntimeException e) {
            throw translateException(e);
        }
    }

    /**
     * translates an Exception if possible. Exceptions that are no {@link RuntimeException}s are wrapped in a
     * RuntimeException
     *
     * @param exception the Exception to map
     * @return the potentially translated RuntimeException.
     * @since 4.0
     */
    private RuntimeException translateException(Exception exception) {

        RuntimeException runtimeException = exception instanceof RuntimeException rtException ? rtException
                : new RuntimeException(exception.getMessage(), exception);
        RuntimeException potentiallyTranslatedException = exceptionTranslator.translateExceptionIfPossible(runtimeException);

        return potentiallyTranslatedException != null ? potentiallyTranslatedException : runtimeException;
    }
}
