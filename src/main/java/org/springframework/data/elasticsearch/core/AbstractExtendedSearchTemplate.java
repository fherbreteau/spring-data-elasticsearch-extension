package org.springframework.data.elasticsearch.core;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.data.convert.EntityReader;
import org.springframework.data.elasticsearch.core.convert.ElasticsearchConverter;
import org.springframework.data.elasticsearch.core.convert.MappingElasticsearchConverter;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.document.SearchDocumentResponse;
import org.springframework.data.elasticsearch.core.event.AfterConvertCallback;
import org.springframework.data.elasticsearch.core.event.AfterLoadCallback;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentEntity;
import org.springframework.data.elasticsearch.core.mapping.ElasticsearchPersistentProperty;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.mapping.SimpleElasticsearchMappingContext;
import org.springframework.data.elasticsearch.core.query.Query;
import org.springframework.data.elasticsearch.core.query.SeqNoPrimaryTerm;
import org.springframework.data.elasticsearch.core.routing.DefaultRoutingResolver;
import org.springframework.data.elasticsearch.core.routing.RoutingResolver;
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public abstract class AbstractExtendedSearchTemplate implements ExtendedSearchOperations, ApplicationContextAware {

    protected final ElasticsearchConverter elasticsearchConverter;
    protected final RoutingResolver routingResolver;

    @Nullable
    protected EntityCallbacks entityCallbacks;

    protected AbstractExtendedSearchTemplate() {
        this(null);
    }

    protected AbstractExtendedSearchTemplate(@Nullable ElasticsearchConverter elasticsearchConverter) {
        this.elasticsearchConverter = elasticsearchConverter != null ? elasticsearchConverter
                : createElasticsearchConverter();
        MappingContext<? extends ElasticsearchPersistentEntity<?>, ElasticsearchPersistentProperty> mappingContext = this.elasticsearchConverter
                .getMappingContext();
        this.routingResolver = new DefaultRoutingResolver(mappingContext);
    }

    private ElasticsearchConverter createElasticsearchConverter() {
        MappingElasticsearchConverter mappingElasticsearchConverter = new MappingElasticsearchConverter(
                new SimpleElasticsearchMappingContext());
        mappingElasticsearchConverter.afterPropertiesSet();
        return mappingElasticsearchConverter;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {

        if (entityCallbacks == null) {
            setEntityCallbacks(EntityCallbacks.create(applicationContext));
        }

        if (elasticsearchConverter instanceof ApplicationContextAware applicationContextAware) {
            applicationContextAware.setApplicationContext(applicationContext);
        }
    }

    /**
     * Set the {@link EntityCallbacks} instance to use when invoking {@link EntityCallbacks callbacks} like the
     * {@link org.springframework.data.elasticsearch.core.event.BeforeConvertCallback}.
     * <p/>
     * Overrides potentially existing {@link EntityCallbacks}.
     *
     * @param entityCallbacks must not be {@literal null}.
     * @throws IllegalArgumentException if the given instance is {@literal null}.
     * @since 4.0
     */
    public void setEntityCallbacks(@Nullable EntityCallbacks entityCallbacks) {

        Assert.notNull(entityCallbacks, "entityCallbacks must not be null");

        this.entityCallbacks = entityCallbacks;
    }

    @SuppressWarnings("java:S4449")
    @Nonnull
    @Override
    public <T> SearchHitsIterator<T> searchForStream(@Nullable Query query, int fromIndex, Class<T> clazz, IndexCoordinates index) {
        Assert.notNull(query, "query must not be null");

        Duration scrollTime = query.getScrollTime();
        if (scrollTime == null) {
            scrollTime = Duration.ofMinutes(1);
        }
        long scrollTimeInMillis = scrollTime.toMillis();
        int maxCount = query.getMaxResults() != null ? query.getMaxResults() : 0;

        return new SkippingSearchHitsIterator<>(maxCount, fromIndex,
                searchScrollStart(scrollTimeInMillis, query, clazz, index),
                scrollId -> searchScrollContinue(scrollId, scrollTimeInMillis, clazz, index),
                this::searchScrollClear);
    }

    /*
     * internal use only, not for public API
     */
    public abstract <T> SearchScrollHits<T> searchScrollStart(long scrollTimeInMillis, Query query, Class<T> clazz,
                                                              IndexCoordinates index);

    /*
     * internal use only, not for public API
     */
    public abstract <T> SearchScrollHits<T> searchScrollContinue(String scrollId, long scrollTimeInMillis,
                                                                 Class<T> clazz, IndexCoordinates index);

    /*
     * internal use only, not for public API
     */
    public void searchScrollClear(String scrollId) {
        searchScrollClear(Collections.singletonList(scrollId));
    }

    /*
     * internal use only, not for public API
     */
    public abstract void searchScrollClear(List<String> scrollIds);

    /**
     * @param clazz the entity class
     * @return the IndexCoordinates defined on the entity.
     * @since 4.0
     */
    public IndexCoordinates getIndexCoordinatesFor(Class<?> clazz) {
        return elasticsearchConverter.getMappingContext().getRequiredPersistentEntity(clazz).getIndexCoordinates();
    }

    @SuppressWarnings({"unchecked", "java:S4449"})
    protected <T> T updateIndexedObject(T entity, IndexedObjectInformation indexedObjectInformation) {

        ElasticsearchPersistentEntity<?> persistentEntity = elasticsearchConverter.getMappingContext()
                .getPersistentEntity(entity.getClass());

        assert persistentEntity != null;
        PersistentPropertyAccessor<Object> propertyAccessor = persistentEntity.getPropertyAccessor(entity);
        ElasticsearchPersistentProperty idProperty = persistentEntity.getIdProperty();

        // Only deal with text because ES generated Ids are strings!
        if (indexedObjectInformation.id() != null && idProperty != null
                && idProperty.getType().isAssignableFrom(String.class)) {
            propertyAccessor.setProperty(idProperty, indexedObjectInformation.id());
        }

        if (indexedObjectInformation.seqNo() != null && indexedObjectInformation.primaryTerm() != null
                && persistentEntity.hasSeqNoPrimaryTermProperty()) {
            ElasticsearchPersistentProperty seqNoPrimaryTermProperty = persistentEntity.getSeqNoPrimaryTermProperty();
            assert seqNoPrimaryTermProperty != null;
            propertyAccessor.setProperty(seqNoPrimaryTermProperty,
                    new SeqNoPrimaryTerm(indexedObjectInformation.seqNo(), indexedObjectInformation.primaryTerm()));
        }

        if (indexedObjectInformation.version() != null && persistentEntity.hasVersionProperty()) {
            ElasticsearchPersistentProperty versionProperty = persistentEntity.getVersionProperty();
            assert versionProperty != null;
            propertyAccessor.setProperty(versionProperty, indexedObjectInformation.version());
        }

        return (T) propertyAccessor.getBean();
    }

    protected <T> SearchDocumentResponse.EntityCreator<T> getEntityCreator(ReadDocumentCallback<T> documentCallback) {
        return searchDocument -> CompletableFuture.completedFuture(documentCallback.doWith(searchDocument));
    }

    protected <T> T maybeCallbackAfterConvert(T entity, Document document, IndexCoordinates index) {

        if (entityCallbacks != null) {
            return entityCallbacks.callback(AfterConvertCallback.class, entity, document, index);
        }

        return entity;
    }

    protected <T> Document maybeCallbackAfterLoad(Document document, Class<T> type, IndexCoordinates indexCoordinates) {

        if (entityCallbacks != null) {
            return entityCallbacks.callback(AfterLoadCallback.class, document, type, indexCoordinates);
        }

        return document;
    }

    // region Document callbacks
    protected interface DocumentCallback<T> {
        @Nonnull
        T doWith(@Nonnull Document document);
    }

    protected class ReadDocumentCallback<T> implements DocumentCallback<T> {
        private final EntityReader<? super T, Document> reader;
        private final Class<T> type;
        private final IndexCoordinates index;

        public ReadDocumentCallback(EntityReader<? super T, Document> reader, Class<T> type, IndexCoordinates index) {

            Assert.notNull(reader, "reader is null");
            Assert.notNull(type, "type is null");

            this.reader = reader;
            this.type = type;
            this.index = index;
        }

        @Nonnull
        public T doWith(@Nonnull Document document) {
            Document documentAfterLoad = maybeCallbackAfterLoad(document, type, index);

            T entity = reader.read(type, documentAfterLoad);

            IndexedObjectInformation indexedObjectInformation = new IndexedObjectInformation(
                    documentAfterLoad.hasId() ? documentAfterLoad.getId() : null,
                    documentAfterLoad.getIndex(),
                    documentAfterLoad.getSeqNo(),
                    documentAfterLoad.getPrimaryTerm(),
                    documentAfterLoad.getVersion());
            entity = updateIndexedObject(entity, indexedObjectInformation);

            return maybeCallbackAfterConvert(entity, documentAfterLoad, index);
        }
    }

    protected interface SearchDocumentResponseCallback<T> {
        @Nonnull
        T doWith(@Nonnull SearchDocumentResponse response);
    }

    protected class ReadSearchScrollDocumentResponseCallback<T> implements SearchDocumentResponseCallback<SearchScrollHits<T>> {
        private final DocumentCallback<T> delegate;
        private final Class<T> type;

        public ReadSearchScrollDocumentResponseCallback(Class<T> type, IndexCoordinates index) {

            Assert.notNull(type, "type is null");

            this.delegate = new ReadDocumentCallback<>(elasticsearchConverter, type, index);
            this.type = type;
        }

        @Nonnull
        @Override
        public SearchScrollHits<T> doWith(@Nonnull SearchDocumentResponse response) {
            List<T> entities = response.getSearchDocuments().stream()
                    .map(delegate::doWith)
                    .toList();
            return (SearchScrollHits<T>) SearchHitMapping.mappingFor(type, elasticsearchConverter).mapHits(response, entities);
        }
    }
}
