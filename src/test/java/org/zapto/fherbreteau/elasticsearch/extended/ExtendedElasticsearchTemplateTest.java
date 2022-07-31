package org.zapto.fherbreteau.elasticsearch.extended;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
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
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

    private List<Hit<EntityAsMap>> createResultHits() {
        return List.of(Hit.of(builder -> builder.index("testEntity").id("id").version(1L)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldReturnAnIteratorWhenSearchingForStream() throws IOException {
        // Given
        SearchResponse<EntityAsMap> response = mock(SearchResponse.class);
        when(client.search(any(SearchRequest.class), eq(EntityAsMap.class))).thenReturn(response);
        HitsMetadata<EntityAsMap> hitsMetadata = HitsMetadata.of(builder -> builder.hits(createResultHits()));
        when(response.hits()).thenReturn(hitsMetadata);
        when(response.scrollId()).thenReturn("ScrollId");

        Query query = new NativeQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .withMaxResults(100)
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchTemplate.searchForStream(query, 0, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();

    }

}
