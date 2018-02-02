package org.aksw.commons.graph.index.core;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Implementation of the {@link SubgraphIsomorphismIndex} without any optimizations.
 * Useful for performance and correctness evaluations.
 * 
 * @author raven Oct 5, 2017
 *
 */
public class SubgraphIsomorphismIndexFlat<K, G, V>
	implements SubgraphIsomorphismIndex<K, G, V>
{
    protected IsoMatcher<G, V> isoMatcher;
    protected SetOps<G, V> setOps;

    protected Map<K, G> keyToGraph = new HashMap<>();
    
    public SubgraphIsomorphismIndexFlat(
            SetOps<G, V> setOps,
            IsoMatcher<G, V> isoMatcher
            ) {
        super();
        this.setOps = setOps;
        this.isoMatcher = isoMatcher;
    }    
    
	@Override
	public void removeKey(Object key) {
		keyToGraph.remove(key);
	}

	@Override
	public K put(K key, G graph) {
		keyToGraph.put(key, graph);
		return key;
	}

	@Override
	public Multimap<K, BiMap<V, V>> lookup(G queryGraph, boolean exactMatch, BiMap<? extends V, ? extends V> baseIso) {
		Multimap<K, BiMap<V, V>> result = HashMultimap.create();
		
//		System.out.println("Query graph: " + queryGraph);
		
		if(baseIso == null) {
			baseIso = HashBiMap.create();
		}
		
		for(Entry<K, G> entry : keyToGraph.entrySet()) {
			K key = entry.getKey();
			G viewGraph = entry.getValue();
//			System.out.println("  View graph: " + viewGraph);
			Iterable<BiMap<V, V>> isos = isoMatcher.match(baseIso, viewGraph, queryGraph);
			Iterator<BiMap<V, V>> it = isos.iterator();
			while(it.hasNext()) {
				BiMap<V, V> tmp = it.next();
//                System.out.println("    Raw Iso: " + tmp);
                //BiMap<V, V> iso = SubgraphIsomorphismIndexImpl.removeIdentity(tmp);
                //System.out.println("    Clean Iso: " + iso);
				result.put(key, tmp);
			}
		}

		return result;
	}

	@Override
	public void printTree() {
		System.out.println(keyToGraph);
	}

	@Override
	public G get(K key) {
		G result = keyToGraph.get(key);
		return result;
	}
}
