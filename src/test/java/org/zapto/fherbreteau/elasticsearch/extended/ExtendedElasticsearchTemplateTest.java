package org.zapto.fherbreteau.elasticsearch.extended;

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
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.client.elc.EntityAsMap;
import org.springframework.data.elasticsearch.client.elc.NativeQueryBuilder;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.core.query.Query;
import org.zapto.fherbreteau.elasticsearch.extended.data.TestEntity;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class ExtendedElasticsearchTemplateTest {

    @Mock
    private ElasticsearchClient client;

    private ExtendedElasticsearchTemplate extendedElasticsearchTemplate;

    @BeforeEach
    public void createExtendedElasticsearchTemplate() {
        ElasticsearchTransport transport = mock(ElasticsearchTransport.class);
        when(client._transport()).thenReturn(transport);
        when(transport.jsonpMapper()).thenReturn(new SimpleJsonpMapper());

        SimpleElasticsearchMappingContext mappingContext = new SimpleElasticsearchMappingContext();
        ElasticsearchConverter elasticsearchConverter = new MappingElasticsearchConverter(mappingContext);
        extendedElasticsearchTemplate = new ExtendedElasticsearchTemplate(client, elasticsearchConverter);
    }

    private List<Hit<EntityAsMap>> createResultHits(int size) {
        List<Hit<EntityAsMap>> results = new ArrayList<>();
        for (int index = 0; index < size; index++) {
            results.add(Hit.of(builder -> builder.index("testEntity").id("id").version(1L)));
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
    public void shouldReturnAnIteratorWhenSearchingForStream() throws IOException {
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
    public void shouldReturnAnIteratorWhenSearchingForStreamFromASpecificIndex() throws IOException {
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
}
