package org.zapto.fherbreteau.elasticsearch.extended.internal;

import org.springframework.data.elasticsearch.client.util.ScrollState;
import org.springframework.data.elasticsearch.core.*;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

public class SkippingSearchHitsIterator<T> implements SearchHitsIterator<T> {

    private final AtomicInteger currentCount = new AtomicInteger();
    private final int maxCount;
    private final int fromIndex;
    private final SearchScrollHits<T> searchHits;
    private final ScrollState scrollState;
    private final Function<String, SearchScrollHits<T>> continueScrollFunction;
    private final Consumer<List<String>> clearScrollConsumer;

    private Iterator<SearchHit<T>> currentScrollHits;
    private int scrollHitsCount;
    private boolean continueScroll = true;
    private boolean isClosed = false;

    public SkippingSearchHitsIterator(int maxCount,
                                      int fromIndex,
                                      SearchScrollHits<T> searchHits,
                                      Function<String, SearchScrollHits<T>> continueScrollFunction,
                                      Consumer<List<String>> clearScrollConsumer) {
        this.maxCount = maxCount;
        this.fromIndex = fromIndex;
        this.searchHits = searchHits;
        this.continueScrollFunction = continueScrollFunction;
        this.clearScrollConsumer = clearScrollConsumer;
        scrollState = new ScrollState(Objects.requireNonNull(searchHits.getScrollId()));

    }

    @Override
    public void close() {
        if (!isClosed) {
            clearScrollConsumer.accept(scrollState.getScrollIds());
            isClosed = true;
        }
    }

    @Override
    @Nullable
    public AggregationsContainer<?> getAggregations() {
        return searchHits.getAggregations();
    }

    @Override
    public float getMaxScore() {
        return searchHits.getMaxScore();
    }

    @Override
    public long getTotalHits() {
        return searchHits.getTotalHits();
    }

    @Override
    @NonNull
    public TotalHitsRelation getTotalHitsRelation() {
        return searchHits.getTotalHitsRelation();
    }

    @Override
    public boolean hasNext() {
        if (currentScrollHits == null) {
            currentScrollHits = searchHits.iterator();
            scrollHitsCount = searchHits.getSearchHits().size();
            continueScroll = currentScrollHits.hasNext();
        }

        // Reach the from Index before starting the real stream.
        // This operation permits to avoid a skip operation in the stream.
        while (!isClosed && continueScroll && currentCount.get() < fromIndex) {
            // Increment the counter with the SearchHits count
            currentCount.addAndGet(scrollHitsCount);
            requestNextPage();
        }

        boolean hasNext = false;

        if (!isClosed && continueScroll && (maxCount <= 0 || currentCount.get() < maxCount)) {
            if (!currentScrollHits.hasNext()) {
                requestNextPage();
            }
            hasNext = currentScrollHits.hasNext();
        }

        if (!hasNext) {
            close();
        }

        return hasNext;
    }

    private void requestNextPage() {
        SearchScrollHits<T> nextPage = continueScrollFunction.apply(scrollState.getScrollId());
        currentScrollHits = nextPage.iterator();
        scrollHitsCount = nextPage.getSearchHits().size();
        scrollState.updateScrollId(nextPage.getScrollId());
        continueScroll = currentScrollHits.hasNext();
    }

    @Override
    public SearchHit<T> next() {
        if (hasNext()) {
            currentCount.incrementAndGet();
            return currentScrollHits.next();
        }
        throw new NoSuchElementException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}
