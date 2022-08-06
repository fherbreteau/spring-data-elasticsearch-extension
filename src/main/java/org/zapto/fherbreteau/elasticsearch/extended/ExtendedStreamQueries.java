package org.zapto.fherbreteau.elasticsearch.extended;

import org.springframework.data.elasticsearch.core.SearchHitsIterator;
import org.springframework.data.elasticsearch.core.SearchScrollHits;
import org.zapto.fherbreteau.elasticsearch.extended.internal.SkippingSearchHitsIterator;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.springframework.util.Assert.isTrue;
import static org.springframework.util.Assert.notNull;

final class ExtendedStreamQueries {

    private ExtendedStreamQueries() {}

    static <T> SearchHitsIterator<T> streamResults(int maxCount,
                                                   int fromIndex,
                                                   SearchScrollHits<T> searchHits,
                                                   Function<String, SearchScrollHits<T>> continueScrollFunction,
                                                   Consumer<List<String>> clearScrollConsumer) {
        isTrue(fromIndex >= 0, "fromIndex must be greater than zero.");
        isTrue(maxCount <= 0 || fromIndex < maxCount, "fromIndex must be less than maxCount if positive.");
        notNull(searchHits, "searchHits must not be null.");
        notNull(searchHits.getScrollId(), "scrollId of searchHits must not be null.");
        notNull(continueScrollFunction, "continueScrollFunction must not be null.");
        notNull(clearScrollConsumer, "clearScrollConsumer must not be null.");

        return new SkippingSearchHitsIterator<>(maxCount, fromIndex, searchHits, continueScrollFunction, clearScrollConsumer);
    }
}
