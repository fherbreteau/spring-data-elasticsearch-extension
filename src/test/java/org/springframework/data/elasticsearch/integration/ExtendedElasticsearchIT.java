package org.springframework.data.elasticsearch.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.client.elc.ExtendedElasticsearchTemplate;
import org.springframework.data.elasticsearch.junit.jupiter.ElasticsearchTemplateConfiguration;
import org.springframework.data.elasticsearch.junit.jupiter.SpringIntegrationTest;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringIntegrationTest
@ContextConfiguration(classes = { ElasticsearchTemplateConfiguration.class })
class ExtendedElasticsearchIT {

    @Autowired
    private ExtendedElasticsearchTemplate extendedSearchTemplate;


    @Test
    @DisplayName("should have an ExtendedElasticsearchTemplate")
    void shouldHaveAElasticsearchTemplat() {
        assertThat(extendedSearchTemplate).isNotNull().isInstanceOf(ExtendedElasticsearchTemplate.class);
    }

}
