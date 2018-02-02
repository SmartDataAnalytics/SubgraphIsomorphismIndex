package org.aksw.commons.graph.index.core;

import java.util.Collection;
import java.util.Set;

import org.aksw.commons.collections.tagmap.TagMap;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;

public class IndexNode<K, G, V, T> {
    // The key associated with the node. null for the root node
    protected K key;

    protected G graph;
    protected Set<T> graphTags;

    // Transitions to target keys
    //protected Multimap<K, Edge<K, G, V, T>> targetKeyToEdges;
    protected Table<K, BiMap<V, V>, Edge<K, G, V, T>> targetKeyToEdges;
    protected TagMap<Edge<K, G, V, T>, T> edgeIndex;


    protected Set<K> parents = Sets.newIdentityHashSet();

    void clearLinks(boolean alsoParents) {
        targetKeyToEdges.clear();
        edgeIndex.clear();
//        if(!edgeIndex.isEmpty()) {
//        	System.out.println("clear failed");
//        	edgeIndex.isEmpty();
//        	edgeIndex.clear();
//        }
        if(alsoParents) {
            parents.clear();
        }
    }

    /**
     * Creates a new node for sub isomorphism indexing.
     *
     * Strategy for indexing the edges to children can be passed via the ctor arg.
     *
     * @param key
     * @param graph
     * @param graphTags
     * @param childIndex
     */
    public IndexNode(K key, G graph, Set<T> graphTags, TagMap<Edge<K, G, V, T>, T> edgeIndex) {
        super();
        this.key = key;
        this.graph = graph;
        this.graphTags = graphTags;
        this.edgeIndex = edgeIndex;
        this.targetKeyToEdges = HashBasedTable.create(); //ArrayListMultimap.create();
    }

    public K getKey() {
        return key;
    }

    public G getGraph() {
        return graph;
    }

    public Set<T> getGraphTags() {
        return graphTags;
    }

    public TagMap<Edge<K, G, V, T>, T> getEdgeIndex() {
        return edgeIndex;
    }
    
    
    public void removeEdge(Edge<K, G, V, T> edge) {
    	K targetKey = edge.getTo();
    	BiMap<V, V> iso = edge.getTransIso();

        edgeIndex.remove(edge);
        targetKeyToEdges.row(targetKey).remove(iso);
    }


    public void appendChild(IndexNode<K, G, V, T> targetNode, G residualGraph, Set<T> residualGraphTags, BiMap<V, V> transIso, BiMap<V, V> baseIso) {

        Edge<K, G, V, T> edge = new Edge<>(this.getKey(), targetNode.getKey(), transIso, residualGraph, residualGraphTags, baseIso);
    	Edge<K, G, V, T> priorEdge = targetKeyToEdges.get(targetNode.getKey(), transIso);
    	
    	if(priorEdge != null) {
    		//System.out.println("Note: Edge already existed; replacing");
            edgeIndex.remove(priorEdge);
    	}

        targetKeyToEdges.put(targetNode.getKey(), transIso, edge);
        
        edgeIndex.put(edge, residualGraphTags);

    	//System.out.println("Appended edge: " + edge);
        
//        Set<Edge<K, G, V, T>> x = Sets.newIdentityHashSet();
//        x.addAll(this.getEdgeIndex().keySet());
//        
//        Set<Edge<K, G, V, T>> y = Sets.newIdentityHashSet();
//        y.addAll(this.getTargetKeyToEdges().values());
//        
//        if(!Objects.equals(x, y)) {
//        	throw new RuntimeException("Not equal");
//        } else {
//        	System.out.println("equals");
//        }

        
        targetNode.getParents().add(key);
    }

//    public Collection<Edge<K, G, V, T>> getEdges() {
//      return keyToChildren.values();
//    }

    public Set<K> getParents() {
        return parents;
    }

    public boolean isLeaf() {
        boolean result = targetKeyToEdges.isEmpty();
        return result;
    }

    public void removeChildById(K targetNodeKey) {
        Collection<Edge<K, G, V, T>> edges = targetKeyToEdges.row(targetNodeKey).values();
        for(Edge<K, G, V, T> edge : edges) {
            edgeIndex.remove(edge);
        }
        targetKeyToEdges.row(targetNodeKey).clear();
    }
    
    @Override
    public String toString() {
    	return "IndexNode [" + key + "]";
    }

//    public Collection<Edge<K, G, V, T>> getEdgesByTargetKey(K targetKey) {
//        return targetKeyToEdges.get(targetKey);
//    }
//
//    public Multimap<K, Edge<K, G, V, T>> getTargetKeyToEdges() {
//        return targetKeyToEdges;
//    }
}
