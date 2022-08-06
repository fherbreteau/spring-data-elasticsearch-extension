package org.zapto.fherbreteau.elasticsearch.extended;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.SearchScrollHits;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExtendedStreamQueriesTest {

    @Mock
    private SearchScrollHits<Object> searchScrollHits;
    @Mock
    private Function<String, SearchScrollHits<Object>> continueScrollFunction;
    @Mock
    private Consumer<List<String>> clearScrollConsumer;

    private SearchHit<Object> createSearchHit() {
        return new SearchHit<>(null, null, null, 0f, null, null, null, null, null, null, new Object());
    }

    @Test
    public void shouldReturnAnIteratorWhenProvidingTheExpectedValues() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();

        // When
        SearchHitsIterator<Object> iterator = ExtendedStreamQueries.streamResults(100, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(iterator).isNotNull().hasNext();

    }

    @Test
    public void shouldReturnAnIteratorWhenProvidingNegativeMaxCount() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        when(searchScrollHits.getSearchHits()).thenReturn(List.of(createSearchHit()));
        when(searchScrollHits.iterator()).thenCallRealMethod();

        // When
        SearchHitsIterator<Object> iterator = ExtendedStreamQueries.streamResults(-1, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer);

        // Then
        assertThat(iterator).isNotNull().hasNext();

    }

    @Test
    public void shouldThrowExceptionWhenFromIndexIsNegative() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> ExtendedStreamQueries.streamResults(100, -1, searchScrollHits, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fromIndex must be greater than zero.");
    }

    @Test
    public void shouldThrowExceptionWhenFromIndexIsGreaterThanAPositiveMaxCount() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> ExtendedStreamQueries.streamResults(1, 2, searchScrollHits, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("fromIndex must be less than maxCount if positive.");
    }

    @Test
    public void shouldThrowExceptionWhenSearchScrollHitsIsNull() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> ExtendedStreamQueries.streamResults(100, 0, null, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("searchHits must not be null.");
    }

    @Test
    public void shouldThrowExceptionWhenScrollIdIsNull() {
        // Given
        // When
        // Then
        assertThatThrownBy(() -> ExtendedStreamQueries.streamResults(100, 0, searchScrollHits, continueScrollFunction, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("scrollId of searchHits must not be null.");
    }

    @Test
    public void shouldThrowExceptionWhenContinueScrollFunctionIsNull() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        // When
        // Then
        assertThatThrownBy(() -> ExtendedStreamQueries.streamResults(100, 0, searchScrollHits, null, clearScrollConsumer))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("continueScrollFunction must not be null.");
    }

    @Test
    public void shouldThrowExceptionWhenClearScrollFunctionIsNull() {
        // Given
        when(searchScrollHits.getScrollId()).thenReturn("ScrollId");
        // When
        // Then
        assertThatThrownBy(() -> ExtendedStreamQueries.streamResults(100, 0, searchScrollHits, continueScrollFunction, null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("clearScrollConsumer must not be null.");
    }
}
