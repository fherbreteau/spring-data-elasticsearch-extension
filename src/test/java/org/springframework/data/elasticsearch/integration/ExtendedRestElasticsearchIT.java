package org.springframework.data.elasticsearch.integration;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.client.erhlc.ExtendedElasticsearchRestTemplate;
import org.springframework.data.elasticsearch.junit.jupiter.ElasticsearchRestTemplateConfiguration;
import org.springframework.data.elasticsearch.junit.jupiter.SpringIntegrationTest;
import org.springframework.test.context.ContextConfiguration;

import static org.assertj.core.api.Assertions.assertThat;

@SpringIntegrationTest
@ContextConfiguration(classes = { ElasticsearchRestTemplateConfiguration.class })
@SuppressWarnings("deprecation")
class ExtendedRestElasticsearchIT {

    @Autowired
    private ExtendedElasticsearchRestTemplate extendedSearchTemplate;


    @Test
    @DisplayName("should have an ExtendedElasticsearchRestTemplate")
    void shouldHaveAElasticsearchTemplat() {
        assertThat(extendedSearchTemplate).isNotNull().isInstanceOf(ExtendedElasticsearchRestTemplate.class);
    }

}
