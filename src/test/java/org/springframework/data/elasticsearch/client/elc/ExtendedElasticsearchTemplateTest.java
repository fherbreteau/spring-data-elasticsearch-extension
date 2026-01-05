package org.springframework.data.elasticsearch.client.elc;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.HitsMetadata;
import co.elastic.clients.elasticsearch.core.search.TotalHitsRelation;
import co.elastic.clients.json.SimpleJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;
import org.springframework.dao.DataAccessException;
import org.springframework.data.annotation.Id;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.event.AfterConvertCallback;
import org.springframework.data.elasticsearch.core.event.AfterLoadCallback;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import org.springframework.data.mapping.callback.EntityCallbacks;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ExtendedElasticsearchTemplateTest {

    @Document(indexName = "test")
    static class TestEntity {
        @Id
        private String id;

        private SeqNoPrimaryTerm primaryTerm;

        TestEntity(String id, SeqNoPrimaryTerm primaryTerm) {
            this.id = id;
            this.primaryTerm = primaryTerm;
        }

        public SeqNoPrimaryTerm getPrimaryTerm() {
            return primaryTerm;
        }

        public void setPrimaryTerm(SeqNoPrimaryTerm primaryTerm) {
            this.primaryTerm = primaryTerm;
        }
    }

    @Mock
    private ElasticsearchClient client;

    @Mock
    private ApplicationContext context;

    @Spy
    private EntityCallbacks entityCallbacks = EntityCallbacks.create();

    @Mock
    private SearchResponse<EntityAsMap> response;
    @Mock
    private ScrollResponse<EntityAsMap> continuation;

    private ExtendedElasticsearchTemplate extendedElasticsearchTemplate;

    @BeforeEach
    void createExtendedElasticsearchTemplate() {
        ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
        when(client._transport()).thenReturn(transport);
        when(transport.jsonpMapper()).thenReturn(new SimpleJsonpMapper());

        extendedElasticsearchTemplate = new ExtendedElasticsearchTemplate(client);
        extendedElasticsearchTemplate.setEntityCallbacks(entityCallbacks);
        extendedElasticsearchTemplate.setApplicationContext(context);
    }

    private List<Hit<EntityAsMap>> createResultHits(int size) {
        List<Hit<EntityAsMap>> results = new ArrayList<>();
        for (int index = 0; index < size; index++) {
            long primaryTerm = index + 1;
            results.add(Hit.of(builder -> builder.index("test")
                    .id(UUID.randomUUID().toString())
                    .version(1L)
                    .seqNo(primaryTerm)
                    .primaryTerm(primaryTerm)));
        }
        return results;
    }

    @Test
    void shouldReturnAnIteratorWhenSearchingForStream() throws IOException {
        // Given
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenReturn(response);
        when(response.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(1))
                .total(thBuilder -> thBuilder.value(1).relation(TotalHitsRelation.Eq))));
        when(response.scrollId()).thenReturn("ScrollId");

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
        verify(entityCallbacks).callback(eq(AfterConvertCallback.class), any(), any(), any());
        verify(entityCallbacks).callback(eq(AfterLoadCallback.class), any(), any(), any());
    }

    @Test
    void shouldReturnAnIteratorWhenSearchingForStreamFromASpecificIndex() throws IOException {
        // Given
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenReturn(response);
        when(response.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(51))
                .total(thBuilder -> thBuilder.value(51).relation(TotalHitsRelation.Eq))));
        when(response.scrollId()).thenReturn("ScrollId");

        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchTemplate.searchForStream(query, 50, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();
        // Verify that iterator has only one result
        assertThat(iterator.next()).isNotNull()
                .extracting("content").isNotNull()
                .isExactlyInstanceOf(TestEntity.class)
                .extracting("id", "primaryTerm")
                .allSatisfy(val -> assertThat(val).isNotNull());
        assertThat(iterator).isExhausted();

        verify(client).clearScroll(any(ClearScrollRequest.class));
        verify(entityCallbacks, atLeast(1)).callback(eq(AfterConvertCallback.class), any(), any(), any());
        verify(entityCallbacks, atLeast(1)).callback(eq(AfterLoadCallback.class), any(), any(), any());
    }

    @Test
    void shouldReturnAnIteratorWhenSearchingForEmptyStream() throws IOException {
        // Given
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenReturn(response);
        when(response.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(0))));
        when(response.scrollId()).thenReturn("Empty");

        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .withMaxResults(200)
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchTemplate.searchForStream(query, 100, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().isExhausted();

        verify(client).clearScroll(any(ClearScrollRequest.class));
    }

    @Test
    void shouldReturnAnIteratorWhenSearchingForStreamFromASpecificIndexWithContinuation() throws IOException {
        // Given
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenReturn(response);
        when(response.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(100))
                .total(thBuilder -> thBuilder.value(101).relation(TotalHitsRelation.Eq))));
        when(response.scrollId()).thenReturn("ScrollId");
        when(client.scroll(any(ScrollRequest.class), eq(EntityAsMap.class))).thenReturn(continuation);
        when(continuation.hits()).thenReturn(HitsMetadata.of(builder -> builder.hits(createResultHits(1))
                .total(thbuilder -> thbuilder.value(101).relation(TotalHitsRelation.Eq))));
        when(continuation.scrollId()).thenReturn("ContinuedScrollId");
        when(client.clearScroll(any(ClearScrollRequest.class)))
                .thenReturn(ClearScrollResponse.of(builder -> builder.succeeded(true).numFreed(2)));

        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
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
