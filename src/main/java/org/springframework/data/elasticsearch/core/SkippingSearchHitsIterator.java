package org.springframework.data.elasticsearch.core;

import org.springframework.data.elasticsearch.client.util.ScrollState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;
import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;

class SkippingSearchHitsIterator<T> implements SearchHitsIterator<T> {

    private final AtomicInteger currentCount = new AtomicInteger();
    private final int maxCount;
    private final int fromIndex;
    private final SearchScrollHits<T> searchHits;
    private final ScrollState scrollState;
    private final Function<String, SearchScrollHits<T>> continueScrollFunction;
    private final Consumer<List<String>> clearScrollConsumer;

    private Iterator<SearchHit<T>> currentScrollHits;
    private int scrollHitsCount;
    private boolean continueScroll;
    private boolean isClosed = false;

    public SkippingSearchHitsIterator(int maxCount,
                                      int fromIndex,
                                      @Nullable SearchScrollHits<T> searchHits,
                                      @Nullable Function<String, SearchScrollHits<T>> continueScrollFunction,
                                      @Nullable Consumer<List<String>> clearScrollConsumer) {
        isTrue(fromIndex >= 0, "fromIndex must be greater than zero.");
        isTrue(maxCount <= 0 || fromIndex < maxCount, "fromIndex must be less than maxCount if positive.");
        this.fromIndex = fromIndex;
        this.maxCount = maxCount;
        notNull(searchHits, "searchHits must not be null.");
        notNull(searchHits.getScrollId(), "scrollId of searchHits must not be null.");
        this.searchHits = searchHits;
        notNull(continueScrollFunction, "continueScrollFunction must not be null.");
        this.continueScrollFunction = continueScrollFunction;
        notNull(clearScrollConsumer, "clearScrollConsumer must not be null.");
        this.clearScrollConsumer = clearScrollConsumer;

        currentScrollHits = searchHits.iterator();
        scrollHitsCount = searchHits.getSearchHits().size();
        scrollState = new ScrollState(requireNonNull(searchHits.getScrollId()));
        continueScroll = currentScrollHits.hasNext();

        // Skip required element
        initIterator();
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
    @Nonnull
    public TotalHitsRelation getTotalHitsRelation() {
        return searchHits.getTotalHitsRelation();
    }

    private void initIterator() {
        while (!isClosed && continueScroll && currentCount.get() < fromIndex) {
            if ((fromIndex - currentCount.get()) > scrollHitsCount) {
                // Increment the counter with the SearchHits count
                currentCount.addAndGet(scrollHitsCount);
                updateInternalDatas(continueScrollFunction.apply(scrollState.getScrollId()));
            } else {
                while (currentCount.get() < fromIndex && currentScrollHits.hasNext()) {
                    currentCount.incrementAndGet();
                    currentScrollHits.next();
                }
            }
        }
    }

    @Override
    public boolean hasNext() {
        boolean hasNext = false;

        if (!isClosed && continueScroll && (maxCount <= 0 || currentCount.get() < maxCount)) {
            if (!currentScrollHits.hasNext()) {
                updateInternalDatas(continueScrollFunction.apply(scrollState.getScrollId()));
            }
            hasNext = currentScrollHits.hasNext();
        }

        if (!hasNext) {
            close();
        }

        return hasNext;
    }

    private void updateInternalDatas(SearchScrollHits<T> nextSearchHits) {
        currentScrollHits = nextSearchHits.iterator();
        scrollHitsCount = nextSearchHits.getSearchHits().size();
        scrollState.updateScrollId(nextSearchHits.getScrollId());
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
}
