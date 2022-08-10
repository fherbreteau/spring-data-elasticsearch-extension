package org.springframework.data.elasticsearch.core;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SkippingSearchHitsIteratorTest {

    @Mock
    private SearchScrollHits<Object> searchScrollHits;

    @Mock
    private Function<String, SearchScrollHits<Object>> continueScrollFunction;

    @Mock
    private Consumer<List<String>> clearScrollConsumer;

    private SearchHit<Object> createSearchHit() {
        return new SearchHit<>(null, null, null, 0f, null, null, null, null, null, null, new Object());
    }

    private List<SearchHit<Object>> createSearchHitList() {
        return IntStream.range(0, 10)
                .mapToObj(index -> createSearchHit())
                .collect(toList());
    }

    @Test
    void shouldInitializeTheIteratorAfterCreation() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
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
    void shouldReturnTheFirstElementAfterCreation() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
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
    void shouldRequestTheNextPageWhenFromIndexIsOnPageTwo() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
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
    void shouldRequestTheNextPageWhenFromIndexIsOnPageThree() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
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
    void shouldFailWhenRequestingATerminatedIterator() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
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
    void shouldRequestTheNextPageWhenPullingAllResults() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
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
    void shouldExtractAggregationsFromInitial() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();
        AggregationsContainer container = mock(AggregationsContainer.class);
        when(searchScrollHits.getAggregations()).thenReturn(container);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getAggregations()).isNotNull();

        skippingSearchHitsIterator.close();
    }

    @Test
    void shouldExtractTotalHitsFromInitial() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();
        when(searchScrollHits.getTotalHits()).thenReturn(100L);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getTotalHits()).isEqualTo(100L);

        skippingSearchHitsIterator.close();
    }

    @Test
    void shouldExtractMaxScoreFromInitial() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();
        when(searchScrollHits.getMaxScore()).thenReturn(1.0f);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getMaxScore()).isEqualTo(1.0f);

        skippingSearchHitsIterator.close();
    }

    @Test
    void shouldExtractTotalHitsRelationFromInitial() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();
        when(searchScrollHits.getTotalHitsRelation()).thenReturn(TotalHitsRelation.EQUAL_TO);

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(skippingSearchHitsIterator.getTotalHitsRelation()).isEqualTo(TotalHitsRelation.EQUAL_TO);

        skippingSearchHitsIterator.close();
    }

    @Test
    void shouldExceptWhenTryingToRemoveElement() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();

        // When
        SkippingSearchHitsIterator<Object> skippingSearchHitsIterator = new SkippingSearchHitsIterator<>(0, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        //Then
        assertThatThrownBy(skippingSearchHitsIterator::remove, "Removing an element should fail")
                .isInstanceOf(UnsupportedOperationException.class);

        skippingSearchHitsIterator.close();
    }

    @Test
    void shouldReturnAnIteratorWhenProvidingTheExpectedValues() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();

        // When
        SearchHitsIterator<Object> iterator = new SkippingSearchHitsIterator<>(100, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(iterator).isNotNull().hasNext();

    }

    @Test
    void shouldReturnAnIteratorWhenProvidingNegativeMaxCount() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();

        // When
        SearchHitsIterator<Object> iterator = new SkippingSearchHitsIterator<>(-1, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(iterator).isNotNull().hasNext();

    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenFromIndexIsNegative() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> new SkippingSearchHitsIterator<>(100, -1, searchScrollHits, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fromIndex must be greater than zero.");
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenFromIndexIsGreaterThanAPositiveMaxCount() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> new SkippingSearchHitsIterator<>(1, 2, searchScrollHits, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fromIndex must be less than maxCount if positive.");
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenSearchScrollHitsIsNull() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> new SkippingSearchHitsIterator<>(100, 0, null, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("searchHits must not be null.");
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenScrollIdIsNull() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> new SkippingSearchHitsIterator<>(100, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("scrollId of searchHits must not be null.");
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenContinueScrollFunctionIsNull() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        // When
        // Then
        assertThatThrownBy(() -> new SkippingSearchHitsIterator<>(100, 0, searchScrollHits, null, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("continueScrollFunction must not be null.");
    }

    @Test
    @SuppressWarnings("resource")
    void shouldThrowExceptionWhenClearScrollFunctionIsNull() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        // When
        // Then
        assertThatThrownBy(() -> new SkippingSearchHitsIterator<>(100, 0, searchScrollHits, continueScrollFunction, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("clearScrollConsumer must not be null.");
    }
}
