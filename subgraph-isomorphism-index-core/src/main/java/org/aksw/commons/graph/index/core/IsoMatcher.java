package org.aksw.commons.graph.index.core;

import com.google.common.collect.BiMap;

public interface IsoMatcher<G, V> {
    Iterable<BiMap<V, V>> match(BiMap<? extends V, ? extends V> baseIso, G a, G b);
}
