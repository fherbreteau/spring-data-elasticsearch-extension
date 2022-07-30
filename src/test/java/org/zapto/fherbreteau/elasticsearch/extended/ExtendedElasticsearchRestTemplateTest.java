package org.zapto.fherbreteau.elasticsearch.extended;

import org.apache.lucene.search.TotalHits;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@SuppressWarnings("deprecation")
public class ExtendedElasticsearchRestTemplateTest {

    @Mock
    private RestHighLevelClient client;

    private ExtendedElasticsearchRestTemplate extendedElasticsearchRestTemplate;

    @BeforeEach
    public void createExtendedElasticsearchTemplate() {
        extendedElasticsearchRestTemplate = new ExtendedElasticsearchRestTemplate(client, null);
    }

    private SearchHit createSearchHit() {
        SearchHit searchHit = new SearchHit(0);
        searchHit.version(1L);
        return searchHit;
    }

    @Test
    public void shouldReturnAnIteratorWhenSearchingForStream() throws IOException {
        // Given
        SearchResponse response = mock(SearchResponse.class);
        SearchHits searchHits = mock(SearchHits.class);
        when(client.search(any(), eq(RequestOptions.DEFAULT))).thenReturn(response);
        when(response.getHits()).thenReturn(searchHits);
        when(response.getScrollId()).thenReturn("ScrollId");
        when(searchHits.getTotalHits()).thenReturn(new TotalHits(2500, TotalHits.Relation.EQUAL_TO));
        when(searchHits.getHits()).thenReturn(new SearchHit[] { createSearchHit() });
        when(searchHits.iterator()).thenCallRealMethod();

        Query query = new NativeSearchQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .withMaxResults(100)
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchRestTemplate.searchForStream(query, 0, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();

    }

}
