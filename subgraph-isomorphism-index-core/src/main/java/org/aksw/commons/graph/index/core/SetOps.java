package org.aksw.commons.graph.index.core;

import java.util.function.Function;

import com.google.common.collect.BiMap;

public interface SetOps<S, I> {
    S createNew();
    S intersect(S a, S b);
    S difference(S a, S b);
    S union(S a, S b);

    // S materialize(S a)

    //S transformItems(S a, Function<I, I> itemTransform);

    S transformItems(S a, Function<I, I> nodeTransform);

    /**
     * Apply a (partial) isomorphic mapping
     * Any non-mapped item is treated as if mapped to itself.
     *
     * Note: Given a set {a, b) and the mapping {a -> b}
     * the reverse mapping of b would be ambiguous.
     *
     * The advantage of this method over the generic transformation is,
     * that the resulting set can be merely a view
     * rather than a copy
     *
     * @param a
     * @param itemTransform
     * @return
     */
    default S applyIso(S a, BiMap<I, I> itemTransform) {
        S result = transformItems(a, itemTransform::get);
        return result;
    }

    int size(S g);

    /**
     * Default isEmpty implementation based on size(g) == 0;.
     * It is recommended to ensure that the implementation
     * of this method has O(1) and not O(n) complexity, i.e. that it
     * does not scan all items.
     *
     *
     * @param s
     * @return
     */
    default boolean isEmpty(S s) {
        int size = size(s);
        boolean result = size == 0;
        return result;
    }
}
