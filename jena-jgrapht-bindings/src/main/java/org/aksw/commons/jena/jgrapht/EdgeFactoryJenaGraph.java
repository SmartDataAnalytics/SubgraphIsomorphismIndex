package org.aksw.commons.jena.jgrapht;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.jgrapht.EdgeFactory;


public class EdgeFactoryJenaGraph
    implements EdgeFactory<Node, Triple>
{
    protected Node predicate;

    public EdgeFactoryJenaGraph(Node predicate) {
        super();
        this.predicate = predicate;
    }

    @Override
    public Triple createEdge(Node sourceVertex, Node targetVertex) {
        Triple result = new Triple(sourceVertex, predicate, targetVertex);
        return result;
    }
}