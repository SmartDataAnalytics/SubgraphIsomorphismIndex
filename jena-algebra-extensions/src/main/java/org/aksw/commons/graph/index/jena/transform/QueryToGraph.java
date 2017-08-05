package org.aksw.commons.graph.index.jena.transform;

import java.util.function.Supplier;

import org.aksw.jena_sparql_api.algebra.transform.TransformJoinToSequence;
import org.aksw.jena_sparql_api.algebra.transform.TransformMergeProject;
import org.aksw.jena_sparql_api.algebra.transform.TransformReplaceConstants;
import org.aksw.jena_sparql_api.algebra.transform.TransformUnionToDisjunction;
import org.aksw.jena_sparql_api.algebra.utils.AlgebraUtils;
import org.aksw.jena_sparql_api.algebra.utils.ExtendedQueryToGraphVisitor;
import org.aksw.jena_sparql_api.algebra.utils.OpExtConjunctiveQuery;
import org.aksw.jena_sparql_api.algebra.utils.OpUtils;
import org.aksw.jena_sparql_api.utils.Generator;
import org.aksw.jena_sparql_api.utils.VarGeneratorImpl2;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.Transformer;
import org.apache.jena.sparql.core.Var;


public class QueryToGraph {

    public static Op normalizeOp(Op op) {
        op = Transformer.transform(TransformUnionToDisjunction.fn, op);
        op = Transformer.transform(TransformJoinToSequence.fn, op);
        //op = Transformer.transform(new TransformReplaceConstants(), op);
        op = TransformReplaceConstants.transform(op);
        //System.out.println("before:" + op);
        op = TransformMergeProject.transform(op);
        //System.out.println("after:" + op);

        Generator<Var> generatorCache = VarGeneratorImpl2.create();
        //op = OpUtils.substitute(op, false, (o) -> SparqlCacheUtils.tryCreateCqfp(o, generatorCache));
        op = OpUtils.substitute(op, false, (o) -> AlgebraUtils.tryCreateCqfp(o, generatorCache));
        return op;
    }

    public static Graph queryToGraph(String queryStr) {
        Graph result;

        Query query;
        try {
            query = QueryFactory.create(queryStr);//SparqlQueryParserImpl.create().apply(queryStr);
        } catch(Exception e) {
            throw new RuntimeException("Failed to parse: " + queryStr, e);
        }
        Op op = Algebra.toQuadForm(Algebra.compile(query));
        Op nop = normalizeOp(op);


        // Collect all conjunctive queries
        if(!(nop instanceof OpExtConjunctiveQuery)) {
            //System.out.println("Not a conjunctive query - skipping");
            throw new RuntimeException("Not a conjunctive query - skipping");
        } else {

            OpExtConjunctiveQuery ocq = (OpExtConjunctiveQuery)nop;
            //ConjunctiveQuery cq = SparqlCacheUtils.tryExtractConjunctiveQuery(op, generator)

            //System.out.println("indexing: " + ocq.getQfpc());

            Supplier<Supplier<Node>> ssn = () -> { int[] x = {0}; return () -> NodeFactory.createBlankNode("_" + x[0]++); };
            QueryToGraphVisitor q2g = new ExtendedQueryToGraphVisitor(ssn.get());
            q2g.visit(ocq);
            result = q2g.getGraph();
        }

        return result;
    }


    public static Node unionMember = NodeFactory.createURI("http://ex.org/unionMember");
    public static Node quadBlockMember = NodeFactory.createURI("http://ex.org/quadBlockMember");
    public static Node filtered = NodeFactory.createURI("http://ex.org/filtered");
//
//
//    //public static unionToGraph(DirectedGraph<Node, LabeledEdge<Node, Node>>)
//
//    public static void addEdge(DirectedGraph<Node, LabeledEdge<Node, Node>> graph, Node edgeLabel, Node source, Node target) {
//        graph.addVertex(source);
//        graph.addVertex(target);
//        graph.addEdge(source, target, new LabeledEdgeImpl<>(source, target, edgeLabel));
//    }
//
//    public static Node addQuad(DirectedGraph<Node, LabeledEdge<Node, Node>> graph, Quad quad) {
//        // Allocate a fresh node for the quad
//        Node quadNode = NodeFactory.createBlankNode();
//        addEdge(graph, Vars.s, quad.getSubject(), quadNode);
//        addEdge(graph, Vars.p, quadNode, quad.getPredicate());
//        addEdge(graph, Vars.o, quadNode, quad.getObject());
//        addEdge(graph, Vars.g, quadNode, quad.getGraph());
//
//        return quadNode;
//    }
//
//
//    /**
//     * Connects every quad's node to a newly allocated node representing the quad block
//     *
//     * @param graph
//     * @param quads
//     * @return
//     */
//    public static Node quadsToGraphNode(DirectedGraph<Node, LabeledEdge<Node, Node>> graph, Collection<Quad> quads) {
//        Node quadBlockNode = NodeFactory.createBlankNode();
//        //graph = new DirectedPseudograph<>(LabeledEdgeImpl.class);
//        for(Quad quad : quads) {
//            Node quadNode = addQuad(graph, quad);
//            addEdge(graph, quadBlockMember, quadBlockNode, quadNode);
//        }
//
//        return quadBlockNode;
//    }
//
//
//    public static void quadsToGraph(DirectedGraph<Node, LabeledEdge<Node, Node>> graph, Collection<Quad> quads) {
//        //graph = new DirectedPseudograph<>(LabeledEdgeImpl.class);
//        for(Quad quad : quads) {
//            addQuad(graph, quad);
//        }
//    }
//
//    // Filters: Extract all equality filters
//    public static void equalExprsToGraph(DirectedGraph<Node, LabeledEdge<Node, Node>> graph, Collection<? extends Collection<? extends Expr>> dnf) {
//        Set<Map<Var, NodeValue>> maps = DnfUtils.extractConstantConstraints(dnf);
//
//        for(Map<Var, NodeValue> map : maps) {
//            // Create a blank node for each clause
//            Node orNode = NodeFactory.createBlankNode();
//            graph.addVertex(orNode);
//
//            for(Entry<Var, NodeValue> e : map.entrySet()) {
//                // Create another blank node for each equality instance
//                // TODO This would be another type of construction: Actually the edge labels are already sufficient for discrimination of equals expressions
//                boolean createNodesForEqualities = true; // Does not work with false as search space gets too big
//                if(createNodesForEqualities) {
//                    Node equalsNode = NodeFactory.createBlankNode();
//                    graph.addVertex(equalsNode);
//
//                    Var v = e.getKey();
//                    Node c = e.getValue().getNode();
//
//                    graph.addVertex(v);
//                    graph.addVertex(c);
//
//                    graph.addEdge(equalsNode, v, new LabeledEdgeImpl<Node, Node>(equalsNode, v, Vars.x));
//                    graph.addEdge(c, equalsNode, new LabeledEdgeImpl<Node, Node>(c, equalsNode, Vars.y));
//
//                    graph.addEdge(orNode, equalsNode, new LabeledEdgeImpl<Node, Node>(orNode, equalsNode, Vars.z));
//
//                } else {
//
//                    Var v = e.getKey();
//                    Node c = e.getValue().getNode();
//
//                    graph.addVertex(v);
//                    graph.addVertex(c);
//
//                    graph.addEdge(orNode, v, new LabeledEdgeImpl<Node, Node>(orNode, v, Vars.x));
//                    graph.addEdge(c, orNode, new LabeledEdgeImpl<Node, Node>(c, orNode, Vars.y));
//                }
//            }
//        }
//    }
//
//
//
//
//
//    public static Stream<Map<Var, Var>> match(DirectedGraph<Node, LabeledEdge<Node, Node>> a, DirectedGraph<Node, LabeledEdge<Node, Node>> b) {
//
////		System.out.println("EDGES:");
////		a.edgeSet().forEach(System.out::println);
////		System.out.println("done with edges");
//
//        Comparator<Node> nodeCmp = (x, y) -> {
//            int  r = (x.isVariable() && y.isVariable()) || (x.isBlank() && y.isBlank()) ? 0 : x.toString().compareTo(y.toString());
//            //System.out.println("" + x + " - " + y + ": " + r);
//            return r;
//        };
//        Comparator<LabeledEdge<Node, Node>> edgeCmp = (x, y) -> x.getLabel().toString().compareTo(y.getLabel().toString());
////		Comparator<//LabeledEd>
//        VF2SubgraphIsomorphismInspector<Node, LabeledEdge<Node, Node>> inspector = new VF2SubgraphIsomorphismInspector<>(b, a, nodeCmp, edgeCmp, true);
//        Iterator<GraphMapping<Node, LabeledEdge<Node, Node>>> it = inspector.getMappings();
//
//        Stream<Map<Var, Var>> result = Streams.stream(it)
//                .map(x -> (IsomorphicGraphMapping<Node, LabeledEdge<Node, Node>>)x)
//                .map(x -> {
//                    Map<Var, Var> varMap = new HashMap<>();
//                    boolean r = true;
//                    for(Node node : b.vertexSet()) {
//                        if(node.isVariable()) {
//                            Var s = (Var)node;
//                            if(x.hasVertexCorrespondence(s)) {
//                                Node fff = x.getVertexCorrespondence(s, true);
//                                if(fff.isVariable()) {
//                                    varMap.put(s, (Var)fff);
//                                } else {
//                                    r = false;
//                                    break;
//                                }
//                            }
//                        }
//                    }
//
//                    Map<Var, Var> s = r ? varMap : null;
//                    return s;
//                }).
//                filter(x -> x != null);
//
//        return result;
//    }

}
