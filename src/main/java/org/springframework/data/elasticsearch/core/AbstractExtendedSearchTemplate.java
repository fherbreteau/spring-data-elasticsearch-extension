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
import org.springframework.data.mapping.PersistentPropertyAccessor;
import org.springframework.data.mapping.callback.EntityCallbacks;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import org.springframework.util.Assert;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public abstract class AbstractExtendedSearchTemplate implements ExtendedSearchOperations, ApplicationContextAware {

    protected final ElasticsearchConverter elasticsearchConverter;
    @Nullable protected EntityCallbacks entityCallbacks;

    protected AbstractExtendedSearchTemplate() {
        this(null);
    }

    protected AbstractExtendedSearchTemplate(@Nullable ElasticsearchConverter elasticsearchConverter) {
        this.elasticsearchConverter = elasticsearchConverter != null ? elasticsearchConverter
                : createElasticsearchConverter();
    }

    private ElasticsearchConverter createElasticsearchConverter() {
        MappingElasticsearchConverter mappingElasticsearchConverter = new MappingElasticsearchConverter(
                new SimpleElasticsearchMappingContext());
        mappingElasticsearchConverter.afterPropertiesSet();
        return mappingElasticsearchConverter;
    }

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) throws BeansException {

        if (entityCallbacks == null) {
            setEntityCallbacks(EntityCallbacks.create(applicationContext));
        }

        if (elasticsearchConverter instanceof ApplicationContextAware) {
            ((ApplicationContextAware) elasticsearchConverter).setApplicationContext(applicationContext);
        }
    }

    /**
     * Set the {@link EntityCallbacks} instance to use when invoking {@link EntityCallbacks callbacks} like the
     * {@link org.springframework.data.elasticsearch.core.event.BeforeConvertCallback}.
     * <p />
     * Overrides potentially existing {@link EntityCallbacks}.
     *
     * @param entityCallbacks must not be {@literal null}.
     * @throws IllegalArgumentException if the given instance is {@literal null}.
     * @since 4.0
     */
    public void setEntityCallbacks(EntityCallbacks entityCallbacks) {

        Assert.notNull(entityCallbacks, "entityCallbacks must not be null");

        this.entityCallbacks = entityCallbacks;
    }

    @Override
    public <T> SearchHitsIterator<T> searchForStream(Query query, int fromIndex, Class<T> clazz, IndexCoordinates index) {

        Duration scrollTime = query.getScrollTime() != null ? query.getScrollTime() : Duration.ofMinutes(1);
        long scrollTimeInMillis = scrollTime.toMillis();
        // noinspection ConstantConditions
        int maxCount = query.isLimiting() ? query.getMaxResults() : 0;

        return new SkippingSearchHitsIterator<>(maxCount, fromIndex,
            searchScrollStart(scrollTimeInMillis, query, clazz, index),
            scrollId -> searchScrollContinue(scrollId, scrollTimeInMillis, clazz, index),
            this::searchScrollClear);
    }

    /*
     * internal use only, not for public API
     */
    protected abstract <T> SearchScrollHits<T> searchScrollStart(long scrollTimeInMillis, Query query, Class<T> clazz,
                                                                 IndexCoordinates index);

    /*
     * internal use only, not for public API
     */
    protected abstract <T> SearchScrollHits<T> searchScrollContinue(String scrollId, long scrollTimeInMillis,
                                                                    Class<T> clazz, IndexCoordinates index);

    /*
     * internal use only, not for public API
     */
    protected void searchScrollClear(String scrollId) {
        searchScrollClear(Collections.singletonList(scrollId));
    }

    /*
     * internal use only, not for public API
     */
    protected abstract void searchScrollClear(List<String> scrollIds);

    /**
     * @param clazz the entity class
     * @return the IndexCoordinates defined on the entity.
     * @since 4.0
     */
    public IndexCoordinates getIndexCoordinatesFor(Class<?> clazz) {
        return getRequiredPersistentEntity(clazz).getIndexCoordinates();
    }

    @SuppressWarnings("unchecked")
    protected <T> T updateIndexedObject(T entity, IndexedObjectInformation indexedObjectInformation) {

        ElasticsearchPersistentEntity<?> persistentEntity = elasticsearchConverter.getMappingContext()
                .getPersistentEntity(entity.getClass());

        if (persistentEntity != null) {
            PersistentPropertyAccessor<Object> propertyAccessor = persistentEntity.getPropertyAccessor(entity);
            ElasticsearchPersistentProperty idProperty = persistentEntity.getIdProperty();

            // Only deal with text because ES generated Ids are strings!
            if (indexedObjectInformation.getId() != null && idProperty != null
                    && idProperty.getType().isAssignableFrom(String.class)) {
                propertyAccessor.setProperty(idProperty, indexedObjectInformation.getId());
            }

            if (indexedObjectInformation.getSeqNo() != null && indexedObjectInformation.getPrimaryTerm() != null
                    && persistentEntity.hasSeqNoPrimaryTermProperty()) {
                ElasticsearchPersistentProperty seqNoPrimaryTermProperty = persistentEntity.getSeqNoPrimaryTermProperty();
                propertyAccessor.setProperty(seqNoPrimaryTermProperty,
                        new SeqNoPrimaryTerm(indexedObjectInformation.getSeqNo(), indexedObjectInformation.getPrimaryTerm()));
            }

            if (indexedObjectInformation.getVersion() != null && persistentEntity.hasVersionProperty()) {
                ElasticsearchPersistentProperty versionProperty = persistentEntity.getVersionProperty();
                propertyAccessor.setProperty(versionProperty, indexedObjectInformation.getVersion());
            }

            return (T) propertyAccessor.getBean();
        }
        return entity;
    }

    protected ElasticsearchPersistentEntity<?> getRequiredPersistentEntity(Class<?> clazz) {
        return elasticsearchConverter.getMappingContext().getRequiredPersistentEntity(clazz);
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
        @Nullable
        T doWith(@Nullable Document document);
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

        @Nullable
        public T doWith(@Nullable Document document) {

            if (document == null) {
                return null;
            }
            Document documentAfterLoad = maybeCallbackAfterLoad(document, type, index);

            T entity = reader.read(type, documentAfterLoad);

            IndexedObjectInformation indexedObjectInformation = IndexedObjectInformation.of(
                    documentAfterLoad.hasId() ? documentAfterLoad.getId() : null,
                    documentAfterLoad.getSeqNo(),
                    documentAfterLoad.getPrimaryTerm(),
                    documentAfterLoad.getVersion());
            entity = updateIndexedObject(entity, indexedObjectInformation);

            return maybeCallbackAfterConvert(entity, documentAfterLoad, index);
        }
    }

    protected interface SearchDocumentResponseCallback<T> {
        @NonNull
        T doWith(@NonNull SearchDocumentResponse response);
    }

    protected class ReadSearchScrollDocumentResponseCallback<T> implements SearchDocumentResponseCallback<SearchScrollHits<T>> {
        private final DocumentCallback<T> delegate;
        private final Class<T> type;

        public ReadSearchScrollDocumentResponseCallback(Class<T> type, IndexCoordinates index) {

            Assert.notNull(type, "type is null");

            this.delegate = new ReadDocumentCallback<>(elasticsearchConverter, type, index);
            this.type = type;
        }

        @NonNull
        @Override
        public SearchScrollHits<T> doWith(@NonNull SearchDocumentResponse response) {
            List<T> entities = response.getSearchDocuments().stream().map(delegate::doWith).collect(Collectors.toList());
            return (SearchScrollHits<T>) SearchHitMapping.mappingFor(type, elasticsearchConverter).mapHits(response, entities);
        }
    }
}
