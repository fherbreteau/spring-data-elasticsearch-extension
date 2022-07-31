package org.zapto.fherbreteau.elasticsearch.extended.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.elasticsearch.core.AggregationsContainer;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.springframework.data.elasticsearch.core.TotalHitsRelation;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class SkippingSearchHitsIteratorTest {

    @Mock
    private SearchScrollHits<Object> searchScrollHits;

    @Mock
    private Function<String, SearchScrollHits<Object>> continueScrollFunction;

    @Mock
    private Consumer<List<String>> clearScrollConsumer;

    @SuppressWarnings("unchecked")
    private List<SearchHit<Object>> createSearchHitList() {
        SearchHit<Object>[] results = new SearchHit[10];
        for (int i = 0; i < results.length; i++) {
                results[i] = new SearchHit<>(null, null, null, 0f, null, null, null, null, null, null, new Object());
        }
        return asList(results);
    }

    @BeforeEach
    public void setupStartSearchScrollHits() {
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
    }

    @Test
    public void shouldInitializeTheIteratorAfterCreation() {
        // Given
        when(searchScrollHits.getSearchHits()).thenReturn(createSearchHitList());
        when(searchScrollHits.iterator()).thenCallRealMethod();

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator).hasNext();
        verifyNoInteractions(continueScrollFunction);
        verifyNoInteractions(clearScrollConsumer);

        skippingSearchHitsIterator.close();
    }

    @Test
    public void shouldReturnTheFirstElementAfterCreation() {
        // Given
        when(searchScrollHits.getSearchHits()).thenReturn(createSearchHitList());
        when(searchScrollHits.iterator()).thenCallRealMethod();

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.next()).isNotNull();
        verifyNoInteractions(continueScrollFunction);
        verifyNoInteractions(clearScrollConsumer);

        skippingSearchHitsIterator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRequestTheNextPageWhenFromIndexIsOnPageTwo() {
        // Given
        when(searchScrollHits.getSearchHits()).thenReturn(createSearchHitList());
        when(searchScrollHits.iterator()).thenCallRealMethod();

        SearchScrollHits<Object> nextPage = mock(SearchScrollHits.class);
        when(nextPage.getScrollId()).thenReturn("ScrollId2");
        when(nextPage.getSearchHits()).thenReturn(createSearchHitList());
        when(nextPage.iterator()).thenCallRealMethod();

        when(continueScrollFunction.apply("ScrollId")).thenReturn(nextPage);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 10, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator).hasNext();
        verify(continueScrollFunction).apply("ScrollId");
        verifyNoInteractions(clearScrollConsumer);

        skippingSearchHitsIterator.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRequestTheNextPageWhenFromIndexIsOnPageThree() {
        // Given
        when(searchScrollHits.getSearchHits()).thenReturn(createSearchHitList());
        when(searchScrollHits.iterator()).thenCallRealMethod();

        SearchScrollHits<Object> nextPage = mock(SearchScrollHits.class);
        when(nextPage.getScrollId()).thenReturn("ScrollId2");
        when(nextPage.getSearchHits()).thenReturn(createSearchHitList());
        when(nextPage.iterator()).thenCallRealMethod();

        SearchScrollHits<Object> nextPage2 = mock(SearchScrollHits.class);
        when(nextPage2.getScrollId()).thenReturn("ScrollId3");
        when(nextPage2.getSearchHits()).thenReturn(emptyList());
        when(nextPage2.iterator()).thenCallRealMethod();


        when(continueScrollFunction.apply("ScrollId")).thenReturn(nextPage);
        when(continueScrollFunction.apply("ScrollId2")).thenReturn(nextPage2);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 20, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator).isExhausted();
        verify(continueScrollFunction).apply("ScrollId");
        verify(continueScrollFunction).apply("ScrollId2");
        verify(clearScrollConsumer).accept(any());

        skippingSearchHitsIterator.close();
        verifyNoMoreInteractions(clearScrollConsumer);
    }


    @Test
    @SuppressWarnings("unchecked")
    public void shouldFailWhenRequestingATerminatedIterator() {
        // Given
        when(searchScrollHits.getSearchHits()).thenReturn(createSearchHitList());
        when(searchScrollHits.iterator()).thenCallRealMethod();

        SearchScrollHits<Object> nextPage = mock(SearchScrollHits.class);
        when(nextPage.getScrollId()).thenReturn("ScrollId2");
        when(nextPage.getSearchHits()).thenReturn(createSearchHitList());
        when(nextPage.iterator()).thenCallRealMethod();

        SearchScrollHits<Object> nextPage2 = mock(SearchScrollHits.class);
        when(nextPage2.getScrollId()).thenReturn("ScrollId3");
        when(nextPage2.getSearchHits()).thenReturn(emptyList());
        when(nextPage2.iterator()).thenCallRealMethod();


        when(continueScrollFunction.apply("ScrollId")).thenReturn(nextPage);
        when(continueScrollFunction.apply("ScrollId2")).thenReturn(nextPage2);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 20, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThatThrownBy(skippingSearchHitsIterator::next)
                .isInstanceOf(NoSuchElementException.class);
        verify(continueScrollFunction).apply("ScrollId");
        verify(continueScrollFunction).apply("ScrollId2");
        verify(clearScrollConsumer).accept(any());

        skippingSearchHitsIterator.close();
        verifyNoMoreInteractions(clearScrollConsumer);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRequestTheNextPageWhenPullingAllResults() {
        // Given
        when(searchScrollHits.getSearchHits()).thenReturn(createSearchHitList());
        when(searchScrollHits.iterator()).thenCallRealMethod();

        SearchScrollHits<Object> nextPage = mock(SearchScrollHits.class);
        when(nextPage.getScrollId()).thenReturn("ScrollId2");
        when(nextPage.getSearchHits()).thenReturn(createSearchHitList());
        when(nextPage.iterator()).thenCallRealMethod();

        SearchScrollHits<Object> nextPage2 = mock(SearchScrollHits.class);
        when(nextPage2.getScrollId()).thenReturn("ScrollId3");
        when(nextPage2.getSearchHits()).thenReturn(emptyList());
        when(nextPage2.iterator()).thenCallRealMethod();


        when(continueScrollFunction.apply("ScrollId")).thenReturn(nextPage);
        when(continueScrollFunction.apply("ScrollId2")).thenReturn(nextPage2);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 10, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator).hasNext();
        verify(continueScrollFunction).apply("ScrollId");

        while (skippingSearchHitsIterator.hasNext()) {
            assertThat(skippingSearchHitsIterator.next()).isNotNull();
        }
        assertThat(skippingSearchHitsIterator).isExhausted();

        verify(continueScrollFunction).apply("ScrollId2");
        verify(clearScrollConsumer).accept(any());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void shouldExtractAggregationsFromInitial() {
        // Given
        AggregationsContainer container = mock(AggregationsContainer.class);
        when(searchScrollHits.getAggregations()).thenReturn(container);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 10, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getAggregations()).isNotNull();
    }

    @Test
    public void shouldExtractTotalHitsFromInitial() {
        // Given
        when(searchScrollHits.getTotalHits()).thenReturn(100L);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 10, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getTotalHits()).isEqualTo(100L);
    }

    @Test
    public void shouldExtractMaxScoreFromInitial() {
        // Given
        when(searchScrollHits.getMaxScore()).thenReturn(1.0f);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 10, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getMaxScore()).isEqualTo(1.0f);
    }

    @Test
    public void shouldExtractTotalHitsRelationFromInitial() {
        // Given
        when(searchScrollHits.getTotalHitsRelation()).thenReturn(TotalHitsRelation.EQUAL_TO);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 10, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getTotalHitsRelation()).isEqualTo(TotalHitsRelation.EQUAL_TO);
    }

    @Test
    public void shouldExceptWhenTryingToRemoveElement() {
        // Given

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 10, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        //Then
        assertThatThrownBy(skippingSearchHitsIterator::remove, "Removing an element should fail")
                .isInstanceOf(UnsupportedOperationException.class);
    }


}
