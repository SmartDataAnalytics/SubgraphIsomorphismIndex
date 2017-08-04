package org.aksw.commons.graph.index.core;

import java.util.function.Function;

import com.google.common.collect.BiMap;
import com.google.common.collect.Multimap;

/**
 *
 * @author raven
 *
 */
public class SubgraphIsomorphismIndexWrapper<K, O, G, V>
    implements SubgraphIsomorphismIndex<K, O, V>
{
    protected SubgraphIsomorphismIndex<K, G, V> index;
    protected Function<O, G> objectToGraph;

    public SubgraphIsomorphismIndexWrapper(SubgraphIsomorphismIndex<K, G, V> index, Function<O, G> objectToGraph) {
        super();
        this.index = index;
        this.objectToGraph = objectToGraph;
    }

    @Override
    public void removeKey(Object key) {
        index.removeKey(key);
    }

    @Override
    public K put(K key, O obj) {
        //K result = remove(key);

        G graph = objectToGraph.apply(obj);
        index.put(key, graph);


        return null;
    }

//    @Override
//    public K add(O obj) {
//        G graph = objectToGraph.apply(obj);
//        K result = index.add(graph);
//        return result;
//    }

    @Override
    public Multimap<K, BiMap<V, V>> lookupX(O queryObj, boolean exactMatch) {
        G graph = objectToGraph.apply(queryObj);

        Multimap<K, BiMap<V, V>> result = index.lookupX(graph, exactMatch);
        return result;
    }

//    @Override
//    public Map<K, Iterable<BiMap<V, V>>> lookupStream(O queryObj, boolean exactMatch) {
//        G graph = objectToGraph.apply(queryObj);
//
//        Map<K, Iterable<BiMap<V, V>>> result = index.lookupStream(graph, exactMatch);
//        return result;
//    }

    @Override
    public void printTree() {
        index.printTree();
    }


    public static <K, O, G, V> SubgraphIsomorphismIndex<K, O, V> wrap(SubgraphIsomorphismIndex<K, G, V> base, Function<O, G> objectToGraph) {
        return new SubgraphIsomorphismIndexWrapper<>(base, objectToGraph);
    }
}
