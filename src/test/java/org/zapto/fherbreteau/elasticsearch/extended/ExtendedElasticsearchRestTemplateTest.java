package org.zapto.fherbreteau.elasticsearch.extended;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.zapto.fherbreteau.elasticsearch.extended.data.TestEntity;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("deprecation")
public class ExtendedElasticsearchRestTemplateTest {

    @Mock
    private RestHighLevelClient client;

    private ExtendedElasticsearchRestTemplate extendedElasticsearchRestTemplate;

    @BeforeEach
    public void createExtendedElasticsearchTemplate() {
        extendedElasticsearchRestTemplate = new ExtendedElasticsearchRestTemplate(client);
    }

    private SearchHit createSearchHit() {
        SearchHit searchHit = new SearchHit(0);
        searchHit.version(1L);
        return searchHit;
    }

    private SearchHit[] createSearchHits(int size) {
        SearchHit[] result = new SearchHit[size];
        for (int index = 0; index < size; index++) {
            result[index] = createSearchHit();
        }
        return result;
    }

    private SearchResponse createResponse(int size, String scrollId) {
        SearchResponse response = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        when(response.getHits()).thenReturn(searchHits);
        when(response.getScrollId()).thenReturn(scrollId);
        when(searchHits.getHits()).thenReturn(createSearchHits(size));
        when(searchHits.iterator()).thenCallRealMethod();
        return response;
    }

    @Test
    public void shouldReturnAnIteratorWhenSearchingForStream() throws IOException {
        // Given
        SearchResponse initialResponse = createResponse(1, "ScrollId");
        when(client.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(initialResponse);
        SearchResponse terminalResponse = createResponse(0, "Empty");
        when(client.scroll(any(), eq(RequestOptions.DEFAULT))).thenReturn(terminalResponse);

        Query query = new NativeSearchQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchRestTemplate.searchForStream(query, 0, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();
        // Verify that iterator has only one result
        assertThat(iterator.next()).isNotNull();
        assertThat(iterator).isExhausted();

        verify(client).clearScroll(any(), eq(RequestOptions.DEFAULT));
    }

    @Test
    public void shouldReturnAnIteratorWhenSearchingForStreamFromASpecificIndex() throws IOException {
        // Given
        SearchResponse initialResponse = createResponse(100, "ScrollId");
        when(client.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(initialResponse);

        SearchResponse continuedResponse = createResponse(1, "ContinuedScrollId");
        SearchResponse terminalResponse = createResponse(0, "Empty");
        when(client.scroll(any(), eq(RequestOptions.DEFAULT))).thenReturn(continuedResponse, terminalResponse);

        Query query = new NativeSearchQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchRestTemplate.searchForStream(query, 100, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();
        // Verify that iterator has only one result
        assertThat(iterator.next()).isNotNull();
        assertThat(iterator).isExhausted();

        verify(client).clearScroll(any(), eq(RequestOptions.DEFAULT));
    }
}
