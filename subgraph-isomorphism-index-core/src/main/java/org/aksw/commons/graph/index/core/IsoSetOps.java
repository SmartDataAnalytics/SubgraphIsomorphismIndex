package org.aksw.commons.graph.index.core;

import com.google.common.collect.BiMap;

public interface IsoSetOps<G, N> {
    G applyIso(G base, BiMap<N, N> iso);
}
