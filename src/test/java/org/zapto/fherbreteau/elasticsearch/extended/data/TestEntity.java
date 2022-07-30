package org.zapto.fherbreteau.elasticsearch.extended.data;

import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;

@Document(indexName = "blog")
public class TestEntity {

    @Id
    private String id;

    private String value;

}
