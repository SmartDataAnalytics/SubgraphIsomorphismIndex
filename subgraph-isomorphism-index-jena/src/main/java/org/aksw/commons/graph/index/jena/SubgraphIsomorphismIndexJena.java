package org.aksw.commons.graph.index.jena;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.aksw.commons.graph.index.core.SubgraphIsomorphismIndex;
import org.aksw.commons.graph.index.core.SubgraphIsomorphismIndexImpl;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.util.NodeUtils;
import org.apache.jena.sparql.util.graph.GraphUtils;
import org.jgrapht.DirectedGraph;
import org.jgrapht.Graph;

import com.google.common.collect.BiMap;
import com.google.common.collect.Streams;

public class SubgraphIsomorphismIndexJena {
    // TODO Move to util class
    public static <K> SubgraphIsomorphismIndex<K, DirectedGraph<Node, Triple>, Node> create() {
        SubgraphIsomorphismIndexImpl<K, DirectedGraph<Node, Triple>, Node, Node> result =
                new SubgraphIsomorphismIndexImpl<>(
                        SetOpsJGraphTRdfJena.INSTANCE,
                        SubgraphIsomorphismIndexJena::extractGraphTags,
                        NodeUtils::compareRDFTerms,
                        new IsoMatcherImpl<>(SubgraphIsomorphismIndexJena::createNodeComparator, SubgraphIsomorphismIndexJena::createEdgeComparator));
        return result;
    }


    public static Comparator<Node> createNodeComparator(BiMap<Node, Node> baseIso) {
        Comparator<Node> result = (x, y) -> compareNodes(baseIso, x, y);
        return result;
    }

    public static Comparator<Triple> createEdgeComparator(BiMap<Node, Node> baseIso) {
        Comparator<Triple> result = (x, y) -> compareNodes(baseIso, x.getPredicate(), y.getPredicate());
        return result;
    }

    public static int compareNodes(BiMap<Node, Node> baseIso, Node i, Node j) {
        int result = (
                        (i.isVariable() && j.isVariable()) ||
                        (i.isBlank() && j.isBlank() ||
                        Objects.equals(baseIso.get(i), j)))
                ? 0
                : NodeUtils.compareRDFTerms(i, j);

        return result;
    }

    public static Set<Node> extractGraphTags(Graph<Node, Triple> graph) {
        Set<Node> result = graph.edgeSet().stream()
            .flatMap(t -> Arrays.asList(t.getSubject(), t.getPredicate(), t.getObject()).stream())
            .filter(n -> n.isURI() || n.isLiteral())
            .collect(Collectors.toSet());

        return result;
    }

    public static Collection<?> extractGraphTags2(org.apache.jena.graph.Graph graph) {
        // TODO: All nodes does not include predicates
        Set<Node> result = Streams.stream(GraphUtils.allNodes(graph))
                .filter(n -> n.isURI() || n.isLiteral())
                .collect(Collectors.toSet());

        return result;
    }
}