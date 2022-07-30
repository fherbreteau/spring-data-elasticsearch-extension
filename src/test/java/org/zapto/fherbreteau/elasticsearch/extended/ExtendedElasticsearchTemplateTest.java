package org.zapto.fherbreteau.elasticsearch.extended;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Pageable;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.data.elasticsearch.core.query.Query;
import org.zapto.fherbreteau.elasticsearch.extended.data.TestEntity;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
public class ExtendedElasticsearchTemplateTest {

    @Mock
    private ElasticsearchClient client;

    private ExtendedElasticsearchTemplate extendedElasticsearchTemplate;

    @BeforeEach
    public void createExtendedElasticsearchTemplate() {
        extendedElasticsearchTemplate = new ExtendedElasticsearchTemplate(client, null);
    }

    @Test
    @Disabled("Runtime failure due to missing dependency")
    public void shouldReturnAnIteratorWhenSearchingForStream() {
        // Given

        Query query = new NativeSearchQueryBuilder()
                .withPageable(Pageable.ofSize(100))
                .withMaxResults(100)
                .build();
        // When
        SearchHitsIterator<TestEntity> iterator = extendedElasticsearchTemplate.searchForStream(query, 0, TestEntity.class);
        // Then
        assertThat(iterator).isNotNull().hasNext();

    }

}
