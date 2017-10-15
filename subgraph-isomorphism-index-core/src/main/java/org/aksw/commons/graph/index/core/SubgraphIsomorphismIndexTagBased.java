package org.aksw.commons.graph.index.core;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.aksw.commons.collections.tagmap.TagMap;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class SubgraphIsomorphismIndexTagBased<K, G, V, T>
	implements SubgraphIsomorphismIndex<K, G, V>
{
    protected IsoMatcher<G, V> isoMatcher;
    protected Function<? super G, Collection<T>> extractGraphTags;
    protected TagMap<K, T> tagMap;
    protected Map<K, G> keyToGraph =  new LinkedHashMap<>();

    
    protected Set<T> extractGraphTagsWrapper(G graph) {
        Collection<T> tmp = extractGraphTags.apply(graph);
        Set<T> result = tmp.stream().collect(Collectors.toSet());
        return result;
    }

    
    public SubgraphIsomorphismIndexTagBased(
    		IsoMatcher<G, V> isoMatcher,
    		Function<? super G, Collection<T>> extractGraphTags,
    		TagMap<K, T> tagMap
            ) {
        super();
        this.isoMatcher = isoMatcher;
        this.extractGraphTags = extractGraphTags;
        this.tagMap = tagMap;
    }    
    
	@Override
	public void removeKey(Object key) {
		tagMap.remove(key);
		keyToGraph.remove(key);
	}

	@Override
	public K put(K key, G graph) {
        if(("" + key).equals("http://lsq.aksw.org/res/q-0a3ec3ad=0") || ("" + key).equals("http://lsq.aksw.org/res/q-0a553057=0")) {
	        System.out.println("HERE: " + key);
	        System.out.println(graph);
	        
	    }
	    
	    
        Set<T> insertGraphTags = extractGraphTagsWrapper(graph);

		tagMap.put(key, insertGraphTags);
		keyToGraph.put(key, graph);
		return key;
	}

	@Override
	public Multimap<K, BiMap<V, V>> lookupX(G queryGraph, boolean exactMatch, BiMap<V, V> baseIso) {
    	if(baseIso == null) {
    		baseIso = HashBiMap.create();
    	}

		
		Multimap<K, BiMap<V, V>> result = HashMultimap.create();

		
        Set<T> insertGraphTags = extractGraphTagsWrapper(queryGraph);

        TagMap<K, T> cands = tagMap.getAllSubsetsOf(insertGraphTags, false);
		
		//BiMap<V, V> baseIso = HashBiMap.create();
		for(K key : cands.keySet()) {
//		    if(("" + key).equals("http://lsq.aksw.org/res/q-0a3ec3ad=0") || ("" + key).equals("http://lsq.aksw.org/res/q-0a553057=0")) {
//		        System.out.println("HERE");
//		    }
		    
			G viewGraph = keyToGraph.get(key);

			Iterable<BiMap<V, V>> isos = isoMatcher.match(baseIso, viewGraph, queryGraph);
			Iterator<BiMap<V, V>> it = isos.iterator();
			while(it.hasNext()) {
				BiMap<V, V> tmp = it.next();
                BiMap<V, V> iso = SubgraphIsomorphismIndexImpl.removeIdentity(tmp);
				result.put(key, iso);
			}
		}

		return result;
	}

	@Override
	public void printTree() {
		System.out.println(tagMap);
	}

	@Override
	public G get(K key) {
		G result = keyToGraph.get(key);
		return result;
	}
}
