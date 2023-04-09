package org.springframework.data.elasticsearch.core;

import org.springframework.data.elasticsearch.client.util.ScrollState;
import org.springframework.util.Assert;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;

class SkippingSearchHitsIterator<T> implements SearchHitsIterator<T> {

    private final AtomicInteger currentCount = new AtomicInteger();
    private final int maxCount;
    private final int fromIndex;

    private final long totalHits;
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
        Assert.isTrue(fromIndex >= 0, "fromIndex must be greater than zero.");
        Assert.isTrue(maxCount <= 0 || fromIndex < maxCount, "fromIndex must be less than maxCount if positive.");
        this.fromIndex = fromIndex;
        this.maxCount = maxCount;
        Assert.notNull(searchHits, "searchHits must not be null.");
        Assert.notNull(searchHits.getScrollId(), "scrollId of searchHits must not be null.");
        this.searchHits = searchHits;
        Assert.notNull(continueScrollFunction, "continueScrollFunction must not be null.");
        this.continueScrollFunction = continueScrollFunction;
        Assert.notNull(clearScrollConsumer, "clearScrollConsumer must not be null.");
        this.clearScrollConsumer = clearScrollConsumer;

        currentScrollHits = searchHits.iterator();
        scrollHitsCount = searchHits.getSearchHits().size();
        scrollState = new ScrollState(Objects.requireNonNull(searchHits.getScrollId()));
        continueScroll = currentScrollHits.hasNext();
        totalHits = searchHits.getTotalHits();

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
        return totalHits;
    }

    @Override
    @Nonnull
    public TotalHitsRelation getTotalHitsRelation() {
        return searchHits.getTotalHitsRelation();
    }

    private void initIterator() {
        while (!isClosed && continueScroll && currentCount.get() < fromIndex) {
            if ((fromIndex - currentCount.get()) >= scrollHitsCount) {
                // Increment the counter with the SearchHits count
                currentCount.addAndGet(scrollHitsCount);
                updateInternalDatas();
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
                updateInternalDatas();
            }
            hasNext = currentScrollHits.hasNext();
        }

        if (!hasNext) {
            close();
        }

        return hasNext;
    }

    private void updateInternalDatas() {
        if (currentCount.get() < totalHits) {
            SearchScrollHits<T> nextSearchHits = continueScrollFunction.apply(scrollState.getScrollId());
            currentScrollHits = nextSearchHits.iterator();
            scrollHitsCount = nextSearchHits.getSearchHits().size();
            scrollState.updateScrollId(nextSearchHits.getScrollId());
            continueScroll = currentScrollHits.hasNext();
        } else {
            currentScrollHits = Collections.emptyIterator();
            scrollHitsCount = 0;
            continueScroll = false;
        }
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
