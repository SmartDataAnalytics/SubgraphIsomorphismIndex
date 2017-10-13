package org.aksw.commons.graph.index.core;

import java.util.Objects;
import java.util.Set;

import com.google.common.collect.BiMap;

class Edge<K, G, V, T>
{
    protected K from;
    protected K to;
    protected BiMap<V, V> transIso;

    // The remaining graph tags at this node
    protected Set<T> residualGraphTags;
    protected G residualGraph;

    protected BiMap<V, V> baseIso;
    
    public Edge(K from, K to, BiMap<V, V> transIso, G residualGraph, Set<T> residualGraphTags, BiMap<V, V> baseIso) {
        super();
        this.from = from;
        this.to = to;
        this.transIso = transIso;
        this.residualGraph = residualGraph;
        this.residualGraphTags = residualGraphTags;
        this.baseIso = baseIso;
        
        
//        if(("" + to).equals("http://lsq.aksw.org/res/q-08237184=0")) {
//	        if(("" + transIso).contains("{}")) {
//	        	System.out.println("dammit1");
//	        }
//	        
//	        String x = ("" + transIso);
//	        if(!x.contains("?value=?val") && x.contains("?obj=?X0") && x.contains("?attrUri=?v_3")) {
//	        	System.out.println("dammit2");
//	        }
//        }

// Potential loops when data like below gets split into residual graphs (i.e. ?obj ?prop ?targetObj) should be prevented
//        (quad <urn:x-arq:DefaultGraphNode> ?obj <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://data.semanticweb.org/ns/swc/ontology#Chair>)
//        (quad <urn:x-arq:DefaultGraphNode> ?obj ?prop ?targetObj)
//        (quad <urn:x-arq:DefaultGraphNode> ?targetObj <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?targetType)        
        if(Objects.equals(from, to)) {
        	throw new RuntimeException("Should not happen");
        }

    }
    
    public BiMap<V, V> getBaseIso() {
		return baseIso;
	}

    public K getFrom() {
        return from;
    }

    public K getTo() {
        return to;
    }

    public BiMap<V, V> getTransIso() {
        return transIso;
    }

    public G getResidualGraph() {
        return residualGraph;
    }

    public Set<T> getResidualGraphTags() {
        return residualGraphTags;
    }

	@Override
	public String toString() {
		return "Edge [from=" + from + ", to=" + to + ", transIso=" + transIso + ", residualGraphTags="
				+ residualGraphTags + ", residualGraph=" + residualGraph + "]";
	}
}