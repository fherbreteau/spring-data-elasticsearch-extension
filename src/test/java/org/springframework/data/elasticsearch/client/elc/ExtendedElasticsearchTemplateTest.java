package org.springframework.data.elasticsearch.client.elc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.json.SimpleJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.Query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExtendedElasticsearchTemplateTest {

    @Document(indexName = "test")
    static class TestEntity{
        @Id
        private String id;
    }

    @Mock
    private ElasticsearchClient client;

    private ExtendedElasticsearchTemplate extendedElasticsearchTemplate;

    @BeforeEach
    public void createExtendedElasticsearchTemplate() {
        ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
        when(client._transport()).thenReturn(transport);
        when(transport.jsonpMapper()).thenReturn(new SimpleJsonpMapper());

        extendedElasticsearchTemplate = new ExtendedElasticsearchTemplate(client);
    }

    private List<Hit<EntityAsMap>> createResultHits(int size) {
        List<Hit<EntityAsMap>> results = new ArrayList<>();
        for (int index = 0; index < size; index++) {
            String id = String.valueOf(index);
            results.add(Hit.of(builder -> builder.index("testEntity").id(id).version(1L)));
        }
        return results;
    }

    @SuppressWarnings("unchecked")
    private ScrollResponse<EntityAsMap> createScrollResponse(int size, String scrollId) {
        ScrollResponse<EntityAsMap> response = mock(ScrollResponse.class);
        when(response.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(size))));
        when(response.scrollId()).thenReturn(scrollId);
        return response;
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnAnIteratorWhenSearchingForStream() throws IOException {
        // Given
        SearchResponse<EntityAsMap> response = mock(SearchResponse.class);
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenReturn(response);
        when(response.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(1))));
        when(response.scrollId()).thenReturn("ScrollId");

        ScrollResponse<EntityAsMap> terminalResponse = createScrollResponse(0, "Empty");
        when(client.scroll(any(ScrollRequest.class), eq(EntityAsMap.class))).thenReturn(terminalResponse);

        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchTemplate.searchForStream(query, 0, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();
        // Verify that iterator has only one result
        assertThat(iterator.next()).isNotNull();
        assertThat(iterator).isExhausted();

        verify(client).clearScroll(any(ClearScrollRequest.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void shouldReturnAnIteratorWhenSearchingForStreamFromASpecificIndex() throws IOException {
        // Given
        SearchResponse<EntityAsMap> response = mock(SearchResponse.class);
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenReturn(response);
        when(response.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(100))));
        when(response.scrollId()).thenReturn("ScrollId");

        ScrollResponse<EntityAsMap> continuedResponse = createScrollResponse(1, "ContinuedScroll");
        ScrollResponse<EntityAsMap> terminalResponse = createScrollResponse(0, "Empty");
        when(client.scroll(any(ScrollRequest.class), eq(EntityAsMap.class))).thenReturn(continuedResponse, terminalResponse);

        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .withMaxResults(200)
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchTemplate.searchForStream(query, 100, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();
        // Verify that iterator has only one result
        assertThat(iterator.next()).isNotNull();
        assertThat(iterator).isExhausted();

        verify(client).clearScroll(any(ClearScrollRequest.class));
    }

    @Test
    void shouldThrowAssertionErrorIfQueryIsNull() {
        assertThatThrownBy(() -> extendedElasticsearchTemplate.searchForStream(null, 0, TestEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("query must not be null");
    }

    @Test
    void shouldThrowAssertionErrorIfPageableOfQueryIsNull() {
        Query query = mock(Query.class);

        assertThatThrownBy(() -> extendedElasticsearchTemplate.searchForStream(query, 0, TestEntity.class))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("pageable of query must not be null.");
    }

    @Test
    void shouldHandleIOExceptionAndTransformItToDataAccessException() throws IOException {
        // Given
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenThrow(IOException.class);
        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .build();

        // When
        assertThatThrownBy(() -> extendedElasticsearchTemplate.searchForStream(query, 0, TestEntity.class))
                .isInstanceOf(DataAccessException.class);
    }

    @Test
    void shouldHandleRuntimeExceptionAndReThrowIt() throws IOException {
        // Given
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenThrow(RuntimeException.class);
        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .build();

        // When
        assertThatThrownBy(() -> extendedElasticsearchTemplate.searchForStream(query, 0, TestEntity.class))
                .isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldNotCallElasticsearchSWhenClearingEmptyScrollIdList() throws IOException {
        // When
        extendedElasticsearchTemplate.searchScrollClear(List.of());

        // Then
        verify(client, times(0)).clearScroll(any(ClearScrollRequest.class));
    }
}
