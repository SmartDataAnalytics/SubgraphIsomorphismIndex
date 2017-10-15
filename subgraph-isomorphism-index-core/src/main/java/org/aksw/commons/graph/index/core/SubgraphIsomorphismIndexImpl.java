package org.aksw.commons.graph.index.core;

import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aksw.commons.collections.tagmap.TagMap;
import org.aksw.commons.collections.tagmap.TagMapSetTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Stopwatch;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.collect.Streams;
import com.google.common.collect.Table;

/**
 * Generic sub graph isomorphism index class that on the surface acts like a Map.
 * The ways the index can be seen are:
 * <ul>
 *   <li>Keys of type K are associated with a graph G with nodes type V.</li>
 *   <li>
 *     Keys of type K are associated with a set G whose items are composed from atoms of type V.
 *     For instance, an RDF graph (G = org.apache.jena.graph.Graph) is a set of triples,
 *     which are composed of (V=org.apache.jena.graph.Node) nodes.
 *   </li>
 * </ul>
 *
 * Type T is for tags, which is a set of static features of G.
 * Typically, this is a set of constants in G;
 * which is invariant - i.e. never remapped - by an isomorphism.
 *
 *
 * The implementation is based on the construction of an isomorphism-based subsumption
 * graph among the (key, graph) pairs.
 * Thereby, the keys (of type K) correspond to nodes in the subsumption graph.
 * Edges represent subsumption relations between nodes and carry the following information
 * about the transition from a source node to a target node:
 * <ul>
 *   <li>
 *     The isomorphism that needs to be applied to reach the target.
 *     I.e. whatever isomorphism existed to reach the source, the edge holds information how to re-map it
 *     to the graph associated with the target.
 *   </li>
 *   <li>
 *     The residual graph between the source and the target under the given isomorphism.
 *     I.e. if a lookup request with a query graph reached the source node, the target node
 *     can only be reached if there exists
 *     an isomorphism between the query graph's residual graph and residual graph associated with the edge.
 *   </li>
 *   <li>
 *     A set of graphTags, which describe static features of the graph. Static features are those that are
 *     unaffected (not subject to being remapped) by the isomorphisms; i.e. constants.
 *     Hence, a query graph must have a super-set of an edge's graph tags for the lookup to traverse that edge.
 *   </li>
 * </ul>
 *
 * The implementation uses a hybrid approach to sub graph indexes comprised of:
 * <ul>
 *   <li>
 *     A generic isomorphism matcher, which is expected to yield possible isomorphisms for two given entities of type G.
 *     Typically, this could be backed by a VF2 implementation
 *   </li>
 *   <li>Tags subset/superset queries are optimized using a set trie implementation.</li>
 * </ul>
 *
 *
 * @author raven
 *
 */
public class SubgraphIsomorphismIndexImpl<K, G, V, T>
    implements SubgraphIsomorphismIndex<K, G, V>
{
    private static final Logger logger = LoggerFactory.getLogger(SubgraphIsomorphismIndexImpl.class);

    /*
     * Customizable fields
     */

    protected IsoMatcher<G, V> isoMatcher;
    protected Function<? super G, Collection<T>> extractGraphTags;
    protected Comparator<? super T> tagComparator;
    protected SetOps<G, V> setOps;


    /*
     * Internal fields
     */
//    protected DirectedGraph<K, Edge<K, G, V, T>> indexGraph = new SimpleDirectedGraph<>(Edge.class);

    protected IndexNode<K, G, V, T> rootNode;
    protected Map<K, IndexNode<K, G, V, T>> keyToNode = new LinkedHashMap<>();

    
    // Index used to find all candidate graphs to which isos need to be computed on insert
    protected TagMap<K, T> graphsByTags;    
    protected Map<K, G> keyToGraph = new HashMap<>();
    
    /**
     * This table maps preferred keys to their alternate keys and the isos from pref keys' graph of that of alt
     * So we can transition from a pref key to all other isomorphic graphs
     */
    protected Table<K, K, Set<BiMap<V, V>>> prefKeyToAltKeysWithIso = HashBasedTable.create();


//    protected boolean enableDebugInfo = false;
//    protected IndentedWriter writer = enableDebugInfo
//            ? IndentedWriter.stderr
//            :  new IndentedWriter(ByteStreams.nullOutputStream());


    public SubgraphIsomorphismIndexImpl(
            SetOps<G, V> setOps,
            Function<? super G, Collection<T>> extractGraphTags,
            Comparator<? super T> tagComparator,
            IsoMatcher<G, V> isoMatcher
            ) {
        super();
        this.setOps = setOps;
        this.extractGraphTags = extractGraphTags;
        this.tagComparator = tagComparator;
        this.isoMatcher = isoMatcher;
        
        this.graphsByTags = new TagMapSetTrie<>(tagComparator);

        rootNode = createNode(null, setOps.createNew(), Collections.emptySet());
        keyToNode.put(null, rootNode);
    }

    protected Set<T> extractGraphTagsWrapper(G graph) {
        Collection<T> tmp = extractGraphTags.apply(graph);
        Set<T> result = tmp.stream().collect(Collectors.toSet());
        return result;
    }


    /* (non-Javadoc)
     * @see org.aksw.jena_sparql_api.iso.index.SubGraphIsomorphismIndex#removeKey(java.lang.Object)
     */
    @Override
    public void removeKey(Object key) {

        // FIXME Make the cast more safe - we should never throw an exception
        K prefKey = getPrefKey((K)key);
        Set<K> altKeys = prefKeyToAltKeysWithIso.row(prefKey).keySet();

        // Remove the key from the alt keys
        altKeys.remove(key);

        graphsByTags.remove(key);

        
        // If the prefKey no longer has any alt keys, remove all nodes associated with that key
        boolean extinguishNodes = altKeys.isEmpty();

        if(extinguishNodes) {
            IndexNode<K, G, V, T> node = keyToNode.get(prefKey);
            extinguishNode(node);
        }
    }

    @Override
    public G get(K key) {
    	K prefKey = getPrefKey(key);
    	
    	IndexNode<K, G, V, T> tmp = keyToNode.get(prefKey);
    	
    	G result = tmp == null ? null : tmp.getGraph();
    	return result;
    }

    /**
     * Create a node with an fresh internal id
     *
     * @param graphIso
     * @return
     */
    protected IndexNode<K, G, V, T> createNode(K key, G graph, Set<T> graphTags) {
        TagMap<Edge<K, G, V, T>, T> tagMap = new TagMapSetTrie<>(tagComparator);
        IndexNode<K, G, V, T> result = new IndexNode<>(key, graph, graphTags, tagMap);
        keyToNode.put(key, result);
        
        return result;
    }


    public Multimap<K, BiMap<V, V>> lookupX(G queryGraph, boolean exactMatch, BiMap<V, V> lookupBaseIso) {
    	if(lookupBaseIso == null) {
    		lookupBaseIso = HashBiMap.create();
    	}
    	
    	Multimap<K, InsertPosition<K, G, V, T>> matches = lookup(lookupBaseIso, queryGraph, exactMatch);

        Multimap<K, BiMap<V, V>> result = newSetMultimap(false, false);//HashMultimap.create();
        //matches.values().map()
//        breadthFirstSearchWithMultipleStartNodesAndOrderedChildren(
//            nodes,
//            node -> prefKeyToNode.reversed().get(node.getId()),
//            prefKeyToGraph,
//            graphToSize, // Create the associated graph, then takes it size
//            node ->
//            node -> Stream.ofNullable(node.getParent()));


        for(Entry<K, Collection<InsertPosition<K, G, V, T>>> match : matches.asMap().entrySet()) {
            K prefKey = match.getKey();

            Map<K, Set<BiMap<V, V>>> altKeys = prefKey == null ? Collections.emptyMap() : prefKeyToAltKeysWithIso.row(prefKey);

            //logger.debug("Alt Keys for " + prefKey + ": " + altKeys);
            
            for(InsertPosition<K, G, V, T> pos : match.getValue()) {
                BiMap<V, V> baseIso = pos.getIso();

                for(Entry<K, Set<BiMap<V, V>>> e : altKeys.entrySet()) {

                    K altKey = e.getKey();
                    //BiMap<V, V> kIso = baseIso;
                    for(BiMap<V, V> transIso : e.getValue()) {
                        //BiMap<V, V> transIso = e.getValue();
                        //transIso = transIso.inverse();
                        // TODO THe transIso is just the delta - we need to assemble it from all parents
                        //System.out.println("Iso from " + prefKey + " to " + altKey + ": " + transIso);
                        BiMap<V, V> altKeyIso = mapDomainVia(baseIso, transIso);
                        //kIso = removeIdentity(kIso);

                        altKeyIso = removeIdentity(altKeyIso);
                        result.put(altKey, altKeyIso);
                    }
                }
            }
        }

        return result;
    }


    /* (non-Javadoc)
     * @see org.aksw.jena_sparql_api.iso.index.SubGraphIsomorphismIndex#lookup(G, boolean)
     */
    //@Override
    public Multimap<K, InsertPosition<K, G, V, T>> lookup(BiMap<V, V> baseIso, G queryGraph, boolean exactMatch) {    	
        Set<T> queryGraphTags = extractGraphTagsWrapper(queryGraph);

        Collection<InsertPosition<K, G, V, T>> positions = new LinkedList<>();
        
        // Create a copy because findInsertPosition will do in-place changes
        BiMap<V, V> baseIsoCopy = HashBiMap.create(baseIso);
        
        findInsertPositions(positions, rootNode, queryGraph, queryGraphTags, null, baseIsoCopy, HashBiMap.create(), true, exactMatch); //, writer);

        Multimap<K, InsertPosition<K, G, V, T>> result = newSetMultimap(false, true);//HashMultimap.create();
        //logger.debug("Lookup result candidates: " + positions.size());
        for(InsertPosition<K, G, V, T> pos : positions) {
            // Match with the children

            result.put(pos.getNode().getKey(), pos);
            //System.out.println("Node " + pos.node + " with keys " + pos.node.getKeys() + " iso: " + pos.getGraphIso().getInToOut());
//            for(K key : pos.node.getKeys()) {
//                result.put(key, pos);
//            }
        }
        return result;
    }





    /**
     * Lookup only pref keys / this skips results isomorphic to the given keys
     * @param queryGraph
     * @param exactMatch
     * @return
     */
    public Multimap<K, BiMap<V, V>> lookupFlat(G queryGraph, boolean exactMatch) {

        Set<T> queryGraphTags = extractGraphTagsWrapper(queryGraph);


        Collection<InsertPosition<K, G, V, T>> positions = new LinkedList<>();

        findInsertPositions(positions, rootNode, queryGraph, queryGraphTags, HashBiMap.create(), HashBiMap.create(), HashBiMap.create(), true, exactMatch); //, writer);

        Multimap<K, BiMap<V, V>> result = newSetMultimap(false, false);//HashMultimap.create();

        if(logger.isDebugEnabled()) {
            logger.debug("Lookup result candidates: " + positions.size());
        }

        for(InsertPosition<K, G, V, T> pos : positions) {
            // Match with the children

            result.put(pos.getNode().getKey(), pos.getIso());
            //System.out.println("Node " + pos.node + " with keys " + pos.node.getKeys() + " iso: " + pos.getGraphIso().getInToOut());
//            for(K key : pos.node.getKeys()) {
//                result.put(key, pos.getIso());
//            }
        }
        return result;
    }

    /* (non-Javadoc)
     * @see org.aksw.jena_sparql_api.iso.index.SubGraphIsomorphismIndex#put(K, G)
     */
    @Override
    public K put(K key, G graph) {
//    	if(("" + key).equals("http://lsq.aksw.org/res/q-1cc48e1a=0")) {
//    		System.out.println("HERE");
//    	}
    	
        Set<T> insertGraphTags = extractGraphTagsWrapper(graph);

        
        keyToGraph.put(key, graph);
        graphsByTags.put(key, insertGraphTags);

        
        add(rootNode, key, graph, insertGraphTags, HashBiMap.create(), HashBiMap.create(), HashBiMap.create(), false); //, writer);

        return key;
    }


    public static <T> Iterable<T> toIterable(Stream<T> stream) {
        Iterable<T> result = () -> stream.iterator();
        return result;
    }

//    /**
//     * Return the subset of BC not covered
//     *
//     * @return
//     */
//    public static BiMap<T, T> isoDiff(isoCD, isoBC) {
//
//    }


//    public boolean isEmpty(G graph) {
//        boolean result = setOps.size(graph) == 0;
//        return result;
//    }


    /**
     * Transitively map all elements in the domain of 'src'
     * { x -> z | x in dom(src) & z = via(src(x)) }
     *
     * FIXME Return a BiMap view instead of a materialized copy
     *
     * @param src
     * @param map
     * @return
     */
    public static <N> BiMap<N, N> mapDomainVia(Map<N, N> src, Map<N, N> map) {
        BiMap<N, N> result = src.entrySet().stream().collect(Collectors.toMap(
                e -> map.getOrDefault(e.getKey(), e.getKey()),
                Entry::getValue,
                (u, v) -> {
                    throw new RuntimeException("should not hapen: " + src + " --- map: " + map);
                },
                HashBiMap::create));
        return result;
    }

    public static <N> BiMap<N, N> mapRangeVia(Map<N, N> src, Map<N, N> map) {
        BiMap<N, N> result = src.entrySet().stream().collect(Collectors.toMap(
                Entry::getKey,
                e -> map.getOrDefault(e.getValue(), e.getValue()),
                (u, v) -> {
                    throw new RuntimeException("should not hapen: " + src + " --- map: " + map);
                },
                HashBiMap::create));
        return result;
    }
//    public static <N> BiMap<N, N> mapDomainViaOld(Map<N, N> src, Map<N, N> map) {
//        BiMap<N, N> result = src.entrySet().stream().collect(Collectors.toMap(
//                e -> map.getOrDefault(e.getKey(), e.getValue()),
//                e -> e.getValue(),
//                (u, v) -> {
//                    throw new RuntimeException("should not hapen: " + src + " --- map: " + map);
//                },
//                HashBiMap::create));
//        return result;
//    }

    /**
     * Creates a new BiMap with identity mappings removed
     * 
     * @param map
     * @return
     */
    public static <T> BiMap<T, T> removeIdentity(Map<T, T> map) {
//    	return HashBiMap.create(map);
        BiMap<T, T> result = map.entrySet().stream()
                .filter(e -> !Objects.equals(e.getKey(), e.getValue()))
                .collect(collectToBiMap(Entry::getKey, Entry::getValue));
        return result;
    }

    public static <T, K, U> Collector<T, ?, BiMap<K, U>> collectToBiMap(Function<? super T, ? extends K> keyMapper, Function<? super T, ? extends U> valueMapper) {
        Collector<T, ?, BiMap<K, U>> x = Collectors.toMap(
                keyMapper, valueMapper,
                (u, v) -> {
                    throw new RuntimeException("should not hapen: " + u + " --- map: " + v);
                },
                HashBiMap::create);
        return x;
    }


    public static <X, Y> BiMap<X, X> chain(Map<X, Y> src, Map<? super Y, X> map) {
        BiMap<X, X> result = HashBiMap.create();
        for(Entry<X, Y> e : src.entrySet()) {
            X k = e.getKey();
            Y l = e.getValue();
            X m = map.get(l);
            if(m != null) {
                //System.out.println("Put: " + k + " -> " + m);
                result.put(k, m);
            }
        }
        return result;
    }


    /**
     * For a given insertGraph, find all nodes in the tree at which insert has to occurr.
     *
     *
     * @param out
     * @param node
     * @param insertGraph
     * @param baseIsoToInsertGraph
     * @param retrievalMode false: only return leaf nodes of insertion, true: return all encountered nodes
     * @param writer
     */
    void findInsertPositionsOld(Collection<InsertPosition<K, G, V, T>> out, IndexNode<K, G, V, T> node, G insertGraph, Set<T> insertGraphTags, BiMap<V, V> nodeBaseIso, BiMap<V, V> baseIsoToInsertGraph, BiMap<V, V> latestIsoAB, boolean retrievalMode, boolean exactMatch) { //, IndentedWriter writer) {
//        System.out.println("findInsertPositions: " + node.getKey() + " " + insertGraphTags);
        
//        writer.println("Finding insert position for user graph of size " + setOps.size(insertGraph));

        // Create the residual set of tags by removing the tags present on the current node from the graphTags

        boolean isSubsumed = false;

        boolean insertAtThisNode = false;



        // Candidate children for recursive lookup of the insert position
        // are those whose tags are subsets of the insertGraphTags
//        writer.incIndent();
        Collection<Edge<K, G, V, T>> candEdges =
                node.getEdgeIndex().getAllSubsetsOf(insertGraphTags, false).keySet();

//        System.out.println("  # candEdges: " + candEdges.size());
//        if(("" + insertGraphTags).contains("http://data.semanticweb.org/ns/swc/ontology#hasProgramme")) {
//            candEdges.forEach(edge -> System.out.println("  Edge: " + edge));
//        }
        
        for(Edge<K, G, V, T> candEdge : candEdges) {
            G viewGraph = candEdge.getResidualGraph();

//            writer.println("Comparison with view graph of size " + setOps.size(viewGraph));

            // For every found isomorphism, check all children whether they are also isomorphic.
//            writer.incIndent();
            int i = 0;

            // Before testing for isomorphy,
            // we need to remap the baseIso with the candidate node's transIso
            // (transIso captures the remapping of set/graph items when moving from the parent to the child node)
            // E.g. if the parent was {(?x a Foo)} and the child is {(?s a Bar)}, then the transIso could be ?x -> ?s
            // if the child node's full graph was { ?s a Foo ; a Bar }.
            BiMap<V, V> childTransIso = candEdge.getTransIso();
            BiMap<V, V> transBaseIso;
            try {
            	transBaseIso = mapDomainVia(baseIsoToInsertGraph, childTransIso);
            } catch(Exception e) {
            	logger.warn("Transferring a detected iso via the iso of a candidate edge failed. Not sure if we can safely ignore this case");
            	continue;
            }

            BiMap<V, V> transNodeBaseIso = null;//mapDomainVia(nodeBaseIso, childTransIso);
//            System.out.println("base: " + nodeBaseIso);
//            System.out.println("trns: "  + childTransIso);
//            System.out.println("ress: " + transNodeBaseIso);
//            transNodeBaseIso.putAll(childTransIso);
            
            
            // FIXME I think using an empty iso here is the correct choice
            //BiMap<V, V> tmpTransBaseIso = HashBiMap.create();
            Iterable<BiMap<V, V>> isos = isoMatcher.match(transBaseIso, viewGraph, insertGraph);

            for(BiMap<V, V> iso : isos) {
                // The difference is all non-identical mappings
                // FIXME We could exclude identical mappings already in the iso matcher
                BiMap<V, V> deltaIso = removeIdentity(iso);

//                writer.println("Found match #" + ++i + ":");
//                writer.incIndent();

                // We need to validate whether the mapping is compatible with the base mapping
                // E.g. if we insert [i1: { ?s ?p ?o }, i2: { ?x a Person }, i3: { ?y a Person ; label ?l}
                // Then there will be two isos from i1 to i3, but only one that is compatible with i2
                boolean isCompatible = MapUtils.isCompatible(iso, transBaseIso);
                if(!isCompatible) {
                    // I think it is perfectly valid for mappings to be incompatible at this stage:
                    // Just because there is an iso between the residual view and insert graph, it does not mean that this is compatible with prior isos between their sub graphs
                	//logger.warn("incompatible mapping");
                	
//                    writer.println("Incompatible:");
//                    writer.println("iso         : " + iso);
//                    writer.println("transBaseIso: " + transBaseIso);
                    continue;
                }

                // A graph is only subsumed if the found iso is compatible with the base iso
                isSubsumed = true;

                // Affected keys are the nodes of the view graph that were newly mapped by the iso
                // We implement a state-space-search approach here: We update the transBaseIso in place
                Set<V> affectedKeys = new HashSet<>(Sets.difference(iso.keySet(), transBaseIso.keySet()));


//                writer.println("affectedkeys: " + affectedKeys);
//                writer.println("iso         : " + iso);
//                writer.println("deltaIso    : " + deltaIso);

                affectedKeys.forEach(k -> transBaseIso.put(k, iso.get(k)));

                G g = setOps.applyIso(viewGraph, iso);

                G residualInsertGraph = setOps.difference(insertGraph, g);

                // now create the diff between the insert graph and mapped child graph
//                writer.println("Diff " + residualInsertGraph + " has "+ setOps.size(residualInsertGraph) + " triples at depth " + writer.getUnitIndent());

                // TODO optimize handling of empty diffs
                K childNodeKey = candEdge.getTo();
                IndexNode<K, G, V, T> childNode = keyToNode.get(childNodeKey);

                Set<T> residualInsertGraphTags = Sets.difference(insertGraphTags, candEdge.getResidualGraphTags());

//                String nodeId = "http://lsq.aksw.org/res/q-00e5a47a=0";
//                if(("" + childNode.getKey()).equals(nodeId)) {
//                	System.out.println("Got node " + nodeId);
//                    System.out.println("Candidates:");
//                    childNode.getEdgeIndex().entrySet().forEach(e -> System.out.println("  Edge: " + e));
//                    try {
//                    Thread.sleep(3000);
//                    } catch(Exception e) {
//                        throw new RuntimeException(e);
//                    }
//                }
                
                findInsertPositions(out, childNode, residualInsertGraph, residualInsertGraphTags, transNodeBaseIso, transBaseIso, deltaIso, retrievalMode, exactMatch); //, writer);

                affectedKeys.forEach(transBaseIso::remove);

//                writer.decIndent();
            }
//            writer.decIndent();
        }
//        writer.decIndent();


        insertAtThisNode = insertAtThisNode || !isSubsumed;
        if(insertAtThisNode || retrievalMode) {
        //if(!isSubsumed || retrievalMode) {

            if(!exactMatch || setOps.isEmpty(insertGraph)) {
//                writer.println("Marking location for insert");
                //System.out.println("keys at node: " + node.getKeys() + " - " + node);
                // Make a copy of the baseIso, as it is transient due to state space search
                InsertPosition<K, G, V, T> pos = new InsertPosition<>(node, insertGraph, insertGraphTags, nodeBaseIso, HashBiMap.create(baseIsoToInsertGraph), latestIsoAB);
                out.add(pos);
            }
        }
    }

    
//    void findInsertPositions2(Collection<InsertPosition<K, G, V, T>> out, IndexNode<K, G, V, T> node, G insertGraph, Set<T> insertGraphTags, BiMap<V, V> nodeBaseIso, BiMap<V, V> baseIsoToInsertGraph, BiMap<V, V> latestIsoAB) {
//    	
//    }
    
    
    
    void findInsertPositions(Collection<InsertPosition<K, G, V, T>> out, IndexNode<K, G, V, T> node, G insertGraph, Set<T> insertGraphTags, BiMap<V, V> nodeBaseIso, BiMap<V, V> baseIsoToInsertGraph, BiMap<V, V> latestIsoAB, boolean retrievalMode, boolean exactMatch) { //, IndentedWriter writer) {
//      System.out.println("findInsertPositions: " + node.getKey() + " " + insertGraphTags);
      
//      writer.println("Finding insert position for user graph of size " + setOps.size(insertGraph));

      // Create the residual set of tags by removing the tags present on the current node from the graphTags

      boolean isSubsumed = false;

      boolean insertAtThisNode = false;


//      if(("" + latestIsoAB).contains("?lastName=?ln") && !("" + latestIsoAB).contains("?firstName=?fn")) {
//    	  System.out.println("Got the case where is goes wrong");
//      }
      

      // Candidate children for recursive lookup of the insert position
      // are those whose tags are subsets of the insertGraphTags
//      writer.incIndent();
      Collection<Edge<K, G, V, T>> candEdges =
              node.getEdgeIndex().getAllSubsetsOf(insertGraphTags, false).keySet();

//      System.out.println("  # candEdges: " + candEdges.size());
//      if(("" + insertGraphTags).contains("http://data.semanticweb.org/ns/swc/ontology#hasProgramme")) {
//          candEdges.forEach(edge -> System.out.println("  Edge: " + edge));
//      }
      
      for(Edge<K, G, V, T> candEdge : candEdges) {
          G viewGraph = candEdge.getResidualGraph();

//          writer.println("Comparison with view graph of size " + setOps.size(viewGraph));

          // For every found isomorphism, check all children whether they are also isomorphic.
//          writer.incIndent();
          int i = 0;

          // Before testing for isomorphy,
          // we need to remap the baseIso with the candidate node's transIso
          // (transIso captures the remapping of set/graph items when moving from the parent to the child node)
          // E.g. if the parent was {(?x a Foo)} and the child is {(?s a Bar)}, then the transIso could be ?x -> ?s
          // if the child node's full graph was { ?s a Foo ; a Bar }.
          BiMap<V, V> childTransIso = candEdge.getTransIso();
          BiMap<V, V> transBaseIso;
          try {
          	transBaseIso = mapDomainVia(baseIsoToInsertGraph, childTransIso);
          } catch(Exception e) {
          	logger.warn("Transferring found iso via iso of a candidate edge failed. Not sure if we can safely ignore this case");
          	continue;
          }

          BiMap<V, V> transNodeBaseIso = null;//mapDomainVia(nodeBaseIso, childTransIso);
//          System.out.println("base: " + nodeBaseIso);
//          System.out.println("trns: "  + childTransIso);
//          System.out.println("ress: " + transNodeBaseIso);
//          transNodeBaseIso.putAll(childTransIso);
          
          
          // FIXME I think using an empty iso here is the correct choice
          //BiMap<V, V> tmpTransBaseIso = HashBiMap.create();
          Iterable<BiMap<V, V>> isos = isoMatcher.match(transBaseIso, viewGraph, insertGraph);

          for(BiMap<V, V> iso : isos) {
              // The difference is all non-identical mappings
              // FIXME We could exclude identical mappings already in the iso matcher
              BiMap<V, V> deltaIso = removeIdentity(iso);

//              writer.println("Found match #" + ++i + ":");
//              writer.incIndent();

              // We need to validate whether the mapping is compatible with the base mapping
              // E.g. if we insert [i1: { ?s ?p ?o }, i2: { ?x a Person }, i3: { ?y a Person ; label ?l}
              // Then there will be two isos from i1 to i3, but only one that is compatible with i2
              boolean isCompatible = MapUtils.isCompatible(iso, transBaseIso);
              if(!isCompatible) {
                  // I think it is perfectly valid for mappings to be incompatible at this stage:
                  // Just because there is an iso between the residual view and insert graph, it does not mean that this is compatible with prior isos between their sub graphs
              	//logger.warn("incompatible mapping");
              	
//                  writer.println("Incompatible:");
//                  writer.println("iso         : " + iso);
//                  writer.println("transBaseIso: " + transBaseIso);
                  continue;
              }

              // A graph is only subsumed if the found iso is compatible with the base iso
              isSubsumed = true;

              // Affected keys are the nodes of the view graph that were newly mapped by the iso
              // We implement a state-space-search approach here: We update the transBaseIso in place
              Set<V> affectedKeys = new HashSet<>(Sets.difference(iso.keySet(), transBaseIso.keySet()));

              

//              writer.println("affectedkeys: " + affectedKeys);
//              writer.println("iso         : " + iso);
//              writer.println("deltaIso    : " + deltaIso);

              affectedKeys.forEach(k -> transBaseIso.put(k, iso.get(k)));

              G g = setOps.applyIso(viewGraph, iso);

              G residualInsertGraph = setOps.difference(insertGraph, g);

              // now create the diff between the insert graph and mapped child graph
//              writer.println("Diff " + residualInsertGraph + " has "+ setOps.size(residualInsertGraph) + " triples at depth " + writer.getUnitIndent());

              // TODO optimize handling of empty diffs
              K childNodeKey = candEdge.getTo();
              IndexNode<K, G, V, T> childNode = keyToNode.get(childNodeKey);

              Set<T> residualInsertGraphTags = Sets.difference(insertGraphTags, candEdge.getResidualGraphTags());

//              String nodeId = "http://lsq.aksw.org/res/q-00e5a47a=0";
//              if(("" + childNode.getKey()).equals(nodeId)) {
//              	System.out.println("Got node " + nodeId);
//                  System.out.println("Candidates:");
//                  childNode.getEdgeIndex().entrySet().forEach(e -> System.out.println("  Edge: " + e));
//                  try {
//                  Thread.sleep(3000);
//                  } catch(Exception e) {
//                      throw new RuntimeException(e);
//                  }
//              }
              
              findInsertPositions(out, childNode, residualInsertGraph, residualInsertGraphTags, transNodeBaseIso, transBaseIso, deltaIso, retrievalMode, exactMatch); //, writer);

              affectedKeys.forEach(transBaseIso::remove);

//              writer.decIndent();
          }
//          writer.decIndent();
      }
//      writer.decIndent();


      insertAtThisNode = insertAtThisNode || !isSubsumed;
      if(insertAtThisNode || retrievalMode) {
      //if(!isSubsumed || retrievalMode) {

          if(!exactMatch || setOps.isEmpty(insertGraph)) {
//              writer.println("Marking location for insert");
              //System.out.println("keys at node: " + node.getKeys() + " - " + node);
              // Make a copy of the baseIso, as it is transient due to state space search
              InsertPosition<K, G, V, T> pos = new InsertPosition<>(node, insertGraph, insertGraphTags, nodeBaseIso, HashBiMap.create(baseIsoToInsertGraph), latestIsoAB);
              out.add(pos);
          }
      }
  }


    /**
     * Test whether the node is a leaf without any associated keys
     *
     * @param node
     * @return
     */
    protected boolean isEmptyLeafNode(IndexNode<K, G, V, T> node) {
        K prefKey = node.getKey();
        Set<K> altKeys = prefKey == null ? Collections.emptySet() : prefKeyToAltKeysWithIso.row(prefKey).keySet();

        boolean result =
                node.isLeaf() && altKeys.isEmpty();

        return result;
    }

//    protected void extinguishNode(long nodeId) {
//        GraphIndexNode<K, G, V, T> node = idToNode.get(nodeId);
//        extinguishNode(node);
//    }

    protected void extinguishNode(IndexNode<K, G, V, T> node) {
        if(node != null && node.getKey() != null) { // Do not extinguish the root node
            // If the node is a child node, then remove it
            if(isEmptyLeafNode(node)) {
                deleteNode(node);

                for(K parentNodeKey : node.getParents()) {
                    IndexNode<K, G, V, T> parentNode = keyToNode.get(parentNodeKey);
                    extinguishNode(parentNode);
                }
            }
        }
    }

    //@Override
    public void deleteNode(IndexNode<K, G, V, T> node) {
        K nodeKey = node.getKey();

        for(K parentNodeKey : node.getParents()) {
            IndexNode<K, G, V, T> parentNode = keyToNode.get(parentNodeKey);

            parentNode.removeChildById(nodeKey);
        }

        keyToNode.remove(nodeKey);
    }

    /**
     * During the insert procedure, the insert graph is never renamed, because we want to figure out
     * how to remap existing nodes such they become a subgraph of the insertGraph.
     *
     * @param graph
     */
    void add(IndexNode<K, G, V, T> node, K key, G insertGraph, Set<T> insertGraphTags, BiMap<V, V> nodeBaseIso, BiMap<V, V> baseIso, BiMap<V, V> deltaIso, boolean forceInsert) { //, IndentedWriter writer) {
        // The insert graph must be larger than the node Graph

    	//System.out.println("Performing addition of " + key + " at node " + node.getKey());

        Collection<InsertPosition<K, G, V, T>> positions = new LinkedList<>();
        findInsertPositions(positions, node, insertGraph, insertGraphTags, nodeBaseIso, baseIso, deltaIso, false, false); //, writer);

        
        // Group insert positions by node and deltaIso
//        Table<K, BiMap<V, V>, InsertPosition<K, G, V, T>> groupedPositions = HashBasedTable.create();
//        for(InsertPosition<K, G, V, T> pos : positions) {
//        	K posKey = pos.getNode().getKey();
//        	BiMap<V, V> dIso = pos.getLatestIsoAB();
//        	groupedPositions.put(posKey, dIso, pos);
//        }
        
        
        
        //System.out.println("Found " + positions.size() + " insert positions for " + key);
//        positions.forEach(p -> {
//            System.out.println("Insert pos: " + p.getNode().getKey() + " --- " + p.getIso());
//        });

    	//Stopwatch sw = Stopwatch.createStarted();

        for(InsertPosition<K, G, V, T> pos : positions) {
            performAdd(key, pos, forceInsert); //, writer);
        }
    	//System.out.println("Performed " + positions.size() + " additions in :" + sw.stop().elapsed(TimeUnit.MILLISECONDS));
    }

    public void printTree() {
        BiMap<V, V> baseIso = HashBiMap.create();
        printTree(rootNode, baseIso, Collections.emptySet(), ""); //, IndentedWriter.stdout);
    }

    public void printTree(IndexNode<K, G, V, T> node, BiMap<V, V> transIso, Set<T> residualTags, String indent) { //, IndentedWriter writer) {

        K prefKey = node.getKey();

        Set<K> altKeys = prefKey == null
                ? Collections.emptySet()
                : prefKeyToAltKeysWithIso.row(prefKey).keySet();

        String line = indent + node.getKey() + " " + altKeys +  " ---  reached via transIso: " + transIso + " --- residualTags: " + residualTags;
        //System.out.println("next line length: " + line.length());
        System.out.println(line);
        
        Collection<Edge<K, G, V, T>> edges = node.getEdgeIndex().keySet();
        //Collection<Edge<K, G, V, T>> edges = node.getTargetKeyToEdges().values();
        
        for(Edge<K, G, V, T> edge : edges) {
            K childKey = edge.getTo();
            IndexNode<K, G, V, T> childNode = keyToNode.get(childKey);
            BiMap<V, V> childTransIso = edge.getTransIso();

            //Set<T> nextTags = Sets.union(baseTags, childNode.getGraphTags());
            Set<T> deltaTags = edge.getResidualGraphTags();
            
            printTree(childNode, childTransIso, deltaTags, indent + "  ");
        }
    }


    /**
     * Reflexive
     * 
     * @param node
     * @param nodeToChildren
     * @return
     */
    public static <T> Stream<T> reachableNodes(T node, Function<T, Stream<T>> nodeToChildren) {
        Stream<T> result = Stream.concat(
                Stream.of(node),
                nodeToChildren.apply(node).flatMap(v -> reachableNodes(v, nodeToChildren)));
        return result;
    }

    /**
     * Perform a lookup of children with tags, thereby adjusting the lookup set
     * while descending
     *
     * @param node
     * @param tags
     * @param adjuster
     * @param nodeToChildren
     * @return
     */
    public static <T, X> Stream<T> lookupChildrenByTags(T node, X tags, BiFunction<T, X, X> adjuster, BiFunction<T, X, Stream<T>> nodeToChildren) {
        Stream<T> result = nodeToChildren.apply(node, tags).flatMap(child -> {
            X adjustedTags = adjuster.apply(child, tags);
            Stream<T> subStream = lookupChildrenByTags(child, adjustedTags, adjuster, nodeToChildren);
            return subStream = Stream.concat(Stream.of(child), subStream);
        });
        return result;
    }




    public static <T, X> Stream<T> lookupProvidedChildrenByTags(Stream<T> children, X tags, BiFunction<T, X, X> adjuster, BiFunction<T, X, Stream<T>> nodeToChildren) {
        Stream<T> result = children.flatMap(child -> {
            X adjustedTags = adjuster.apply(child, tags);
            Stream<T> subStream = lookupChildrenByTags(child, adjustedTags, adjuster, nodeToChildren);
            return subStream = Stream.concat(Stream.of(child), subStream);
        });
        return result;
    }

    public static <T, X> Stream<T> reachableNodesWithParent(T node, X tags, BiFunction<T, X, X> adjuster, BiFunction<T, X, Stream<T>> nodeToChildren) {
        Stream<T> result = Stream.concat(
                Stream.of(node),
                nodeToChildren.apply(node, tags).flatMap(v -> {
                    X adjustedTags = adjuster.apply(node, tags);
                    return lookupChildrenByTags(v, adjustedTags, adjuster, nodeToChildren);
                }));
        return result;
    }

//    public static <T> Stream<T> breadthFirstStream(Collection<T> nodes, Function<T, Stream<T>> nodeToChildren) {
//        return breadthFirstStream(() -> nodes.stream(), nodeToChildren);
//    }

    public static <T> Stream<T> breadthFirstStream(T node, Function<T, Stream<T>> nodeToChildren) {
        Stream<T> result = Stream.concat(
                Stream.of(node),
                nodeToChildren.apply(node).flatMap(child -> breadthFirstStream(child, nodeToChildren)));

        return result;
    }

    /**
     *
     * @param nodes
     * @param nodeToChildren
     * @return
     */
    public static <T> Stream<T> breadthFirstSearchWithMultipleStartNodes(Collection<T> nodes, Function<T, Stream<T>> nodeToChildren) {
        Stream<T> result = Stream.concat(
                nodes.stream(),
                nodes.stream().flatMap(child -> breadthFirstStream(child, nodeToChildren)));

        return result;
    }



    public static <T, K, I extends Comparable<I>> Stream<T> breadthFirstSearchWithMultipleStartNodesAndOrderedChildren(Collection<T> nodes, Function<? super T, ? extends K> nodeToKey, Function<? super T, ? extends I> nodeToSize, Function<T, Stream<T>> nodeToChildren) {

        Set<K> seen = new HashSet<>(); //Sets.newIdentityHashSet();
        SetMultimap<I, T> sizeToNodes = Multimaps.newSetMultimap(new TreeMap<I, Collection<T>>(), () -> Sets.<T>newIdentityHashSet());

        for(T node : nodes) {
            K key = nodeToKey.apply(node);
            if(!seen.contains(key)) {
                seen.add(key);
                I size = nodeToSize.apply(node);
                sizeToNodes.put(size, node);
            }
        }

        Iterator<T> rIt = new AbstractIterator<T>() {
            @Override
            protected T computeNext() {
                T r;

                if(!sizeToNodes.isEmpty()) {
                    Iterator<Entry<I, T>> it = sizeToNodes.entries().iterator();
                    Entry<I, T> e = it.next();
                    it.remove();

                    r = e.getValue();

                    // Add the children of T to the open set
                    nodeToChildren.apply(r).forEach(child -> {
                        if(child != null) {
                            K key = nodeToKey.apply(child);
                            if(!seen.contains(key)) {
                                seen.add(key);
                                I size = nodeToSize.apply(child);
                                sizeToNodes.put(size, child);
                            }
                        }
                    });
                } else {
                    r = this.endOfData();
                }

                return r;
            }
        };


        Stream<T> result = Streams.stream(rIt);

        return result;
    }

    /**
     * There are two options to compute each key's graph in a subtree:
     * (1) We start with each key's complete graph, and then subtract the graph of the node
     * (2) Starting from the node, construct for an arbitrary node with that key the graph
     *
     * Approach 2:
     * For every key, pick a graph among those with a minimum depth (so we need the least number of
     * applyIso+union operations)
     * This means: perform breadth first, and
     *
     */
    public Map<K, Entry<BiMap<V, V>, Entry<G, Set<T>>>> loadGraphsInSubTrees(G baseGraph, Set<T> baseGraphTags, Collection<Edge<K, G, V, T>> edges) {

    	Map<K, Entry<BiMap<V, V>, Entry<G, Set<T>>>> result = new LinkedHashMap<>();
    	

    	for(Edge<K, G, V, T> edge : edges) {
        	BiMap<V, V> transIso = edge.getTransIso();
        	
        	Map<K, Entry<G, Set<T>>> tmp = new LinkedHashMap<>();
            loadGraphsInSubTreesCore(tmp, baseGraph, baseGraphTags, Collections.singleton(edge));
            
            
            for(Entry<K, Entry<G, Set<T>>> x : tmp.entrySet()) {
            	K key = x.getKey();
            	Entry<G, Set<T>> graphAndTags = x.getValue();
            	result.computeIfAbsent(key, (xx) -> {
            		return new SimpleEntry<>(transIso, graphAndTags);
            	});
            }
        }
    	
    	return result;
    }

    public Multimap<K, Edge<K, G, V, T>> loadIsoReachableGraphKeys(Multimap<K, Edge<K, G, V, T>> result, BiMap<V, V> baseIso, Collection<Edge<K, G, V, T>> edges) {
    	for(Edge<K, G, V, T> edge : edges) {
    		K targetNodeKey = edge.getTo();

    		reachableNodes(targetNodeKey, n -> keyToNode.get(n).getEdgeIndex().keySet().stream().map(Edge::getTo))
    		.forEach(k -> result.put(k, edge));
     	}
    	return result;
    }

    public static <K, V> SetMultimap<K, V> newSetMultimap(boolean identityKeys, boolean identityValues) {
        Map<K, Collection<V>> keys = identityKeys ? Maps.newIdentityHashMap() : new LinkedHashMap<>();
        com.google.common.base.Supplier<Set<V>> values = identityValues ? Sets::newIdentityHashSet : LinkedHashSet::new;

        return Multimaps.newSetMultimap(keys, values);
    }

    public void loadIsoReachableGraphKeysOld(Map<K, Multimap<Edge<K, G, V, T>, BiMap<V, V>>> result, Edge<K, G, V, T> baseEdge, BiMap<V, V> baseIso, Edge<K, G, V, T> targetEdge, Set<T> baseResidualTags) {
        K targetNodeKey = targetEdge.getTo();
        BiMap<V, V> transIso = targetEdge.getTransIso();
        Set<T> edgeResidualTags = targetEdge.getResidualGraphTags();
        
//        if(baseIso != null) {
//        	System.out.println("Got base iso: " + baseIso);
//        }
        
        BiMap<V, V> completeIso = baseIso == null ? HashBiMap.create(transIso) : mapRangeVia(baseIso, transIso); //mapDomainVia(baseIso, transIso);
        Set<T> completeResidualTags = Sets.union(edgeResidualTags, baseResidualTags);
        
        Multimap<Edge<K, G, V, T>, BiMap<V, V>> edgeTransIsos = result.computeIfAbsent(targetNodeKey, (k) -> newSetMultimap(true, false));
        Collection<BiMap<V, V>> transIsos = edgeTransIsos.get(baseEdge);

        //transIsos.add(new SimpleEntry<>(completeIso, completeResidualTags));
        transIsos.add(completeIso);


        // Recurse
        IndexNode<K, G, V, T> targetNode = keyToNode.get(targetNodeKey);

        Collection<Edge<K, G, V, T>> childEdges = targetNode.getEdgeIndex().keySet();
        for(Edge<K, G, V, T> childEdge : childEdges) {
            loadIsoReachableGraphKeysOld(result, baseEdge, completeIso, childEdge, completeResidualTags);
        }
    }
    
    public Map<K, Multimap<Edge<K, G, V, T>, BiMap<V, V>>> loadIsoReachableGraphKeysOld(Collection<Edge<K, G, V, T>> edges) {
        //Map<K, Multimap<Edge<K, G, V, T>, Entry<BiMap<V, V>, Set<T>>>> result = new LinkedHashMap<>();
        Map<K, Multimap<Edge<K, G, V, T>, BiMap<V, V>>> result = new LinkedHashMap<>();
        for(Edge<K, G, V, T> baseEdge : edges) {
    	    loadIsoReachableGraphKeysOld(result, baseEdge, null, baseEdge, new LinkedHashSet<>());
    	}
    	
    	return result;
    }

    
    public Multimap<K, BiMap<V, V>> loadIsoReachableGraphKeys2(Collection<Edge<K, G, V, T>> edges) {
        //Map<K, Multimap<Edge<K, G, V, T>, Entry<BiMap<V, V>, Set<T>>>> result = new LinkedHashMap<>();
        Multimap<K, BiMap<V, V>> result = HashMultimap.create();
        for(Edge<K, G, V, T> baseEdge : edges) {
    	    loadIsoReachableGraphKeysOld2(result, null, baseEdge);
    	}
    	
    	return result;
    }
    
    public void loadIsoReachableGraphKeysOld2(Multimap<K, BiMap<V, V>> result, BiMap<V, V> baseIso, Edge<K, G, V, T> targetEdge) {
        K targetNodeKey = targetEdge.getTo();
        BiMap<V, V> transIso = targetEdge.getTransIso();

        // NOTE We must not remove identities in the transIsos here, otherwise we would lose the keys which we intend to remap
        // {?s -> ?x } { ?x -> ?s} -> { ?s -> ?s }
        BiMap<V, V> completeIso = baseIso == null ? HashBiMap.create(transIso) : mapRangeVia(baseIso, transIso); //mapDomainVia(baseIso, transIso);

        Collection<BiMap<V, V>> isos = result.get(targetNodeKey);
        
        
        // Here we can remove identity mappings
        isos.add(removeIdentity(completeIso));


        // Recurse
        IndexNode<K, G, V, T> targetNode = keyToNode.get(targetNodeKey);

        Collection<Edge<K, G, V, T>> childEdges = targetNode.getEdgeIndex().keySet();
        for(Edge<K, G, V, T> childEdge : childEdges) {
            loadIsoReachableGraphKeysOld2(result, completeIso, childEdge);
        }
    }
    
    
    public Map<K, Set<T>> loadReachableGraphTags(Map<K, Set<T>> result, Set<T> baseGraphTags, Collection<Edge<K, G, V, T>> edges) {
        for(Edge<K, G, V, T> edge : edges) {
            K targetNodeKey = edge.getTo();
            
            Set<T> graphTags = result.computeIfAbsent(targetNodeKey, (tgn) -> {
                Set<T> residualGraphTags = edge.getResidualGraphTags();
                Set<T> r = Sets.union(baseGraphTags, residualGraphTags);                
                
                return r;
            });

            // Recurse
            //if(isNonVisistedTarget[0]) {
                IndexNode<K, G, V, T> targetNode = keyToNode.get(targetNodeKey);
    
                Collection<Edge<K, G, V, T>> childEdges = targetNode.getEdgeIndex().keySet();
                //Collection<Edge<K, G, V, T>> childEdges = targetNode.getTargetKeyToEdges().values();
    
                
                loadReachableGraphTags(result, graphTags, childEdges);
            //}
        }

        return result;
    }
    
    public Map<K, Entry<G, Set<T>>> loadGraphsInSubTreesCore(Map<K, Entry<G, Set<T>>> result, G baseGraph, Set<T> baseGraphTags, Collection<Edge<K, G, V, T>> edges) {
        for(Edge<K, G, V, T> edge : edges) {
            K targetNodeKey = edge.getTo();
                        
            
            //boolean isNonVisistedTarget[] = {false};
            
            Entry<G, Set<T>> info = result.computeIfAbsent(targetNodeKey, (tgn) -> {
//                if(("" + targetNodeKey).equals("http://lsq.aksw.org/res/q-00dc64d6=0")) {
//                    System.out.println("HERE: " + edge);
//                }
            	//isNonVisistedTarget[0] = true; 

                BiMap<V, V> transIso = edge.getTransIso();
                G residualGraph = edge.getResidualGraph();
                Set<T> residualGraphTags = edge.getResidualGraphTags();

                G transGraph = setOps.applyIso(baseGraph, transIso);
                G targetResidualGraph = setOps.union(transGraph, residualGraph);
                Set<T> targetResidualGraphTags = Sets.union(baseGraphTags, residualGraphTags);                
                
                Entry<G, Set<T>> r = new SimpleEntry<>(targetResidualGraph, targetResidualGraphTags);
                return r;
            });

            // Recurse
            //if(isNonVisistedTarget[0]) {
	            IndexNode<K, G, V, T> targetNode = keyToNode.get(targetNodeKey);
	
	            Collection<Edge<K, G, V, T>> childEdges = targetNode.getEdgeIndex().keySet();
	            //Collection<Edge<K, G, V, T>> childEdges = targetNode.getTargetKeyToEdges().values();
	
	            G targetGraph = info.getKey();
	            Set<T> targetGraphTags = info.getValue();
	            
	            loadGraphsInSubTreesCore(result, targetGraph, targetGraphTags, childEdges);
            //}
        }

        return result;
    }


    /**
     * Get the list of parents, reverse it and apply transIso and union
     *
     *
     *
     * @param node
     * @return
     */
    // Note: we could do function that assembles the graph for a specific key from a given start node
//    public G assembleGraphAtNode(GraphIndexNode<K, G, V, T> destNode, Predicate<GraphIndexNode<K, G, V, T>> isStartNode) {
//
//        // TODO Support memoization of assembledGraphs
//        G result = isStartNode.test(destNode)
//            ? destNode.getValue()
//            : setOps.union(
//                setOps.applyIso(assembleGraphAtNode(destNode.getParent(), isStartNode), destNode.getTransIso()),
//                destNode.getValue());
//
//        return result;
//    }

    public static <T> void parentsOf(T node, Function<T, T> getParent) {

    }


    protected K getPrefKey(K key) {
        Set<K> prefKeys = prefKeyToAltKeysWithIso.column(key).keySet();
        // Note: by the way the prefKeys set is constructed it must have only at most one entry
        K result = prefKeys.isEmpty()
                ? key
                : prefKeys.iterator().next();
        return result;
    }


    /**
     * There are 3 case for inserting a (k: K, g: Graph) pair at a certain node n:
     * (a) The residual graph g is empty, which means that the key is isomorphic to an existing one
     * (b) n does not have any children
     * (c) n does have children
     *
     *
     * These cases are treated as:
     * (a) The key k is added to the set of keys associated with n
     * (b) Get or create* a node for (k, residual graph) and attach it as a child to n
     * (c) Conceptually:
     *     For every (pref) key in n's sub tree, collect a map from that key to its residual graph (as seen from n)
     *     Order map entries by the size of the residual graphs, smallest first
     *     Then, remove all children from n, and re-insert the entries in order to n.
     *     The in-order insert guarantees, that case (c) cannot occur.
     *
     * * Get or create a node for (k, residual graph):
     *
     * k1: { ?a a Person }
     * k2: { ?b label ?c }
     * k3: { ?s a Person ; label ?o }
     * k4: { ?x a Person ; label ?y ; age ?z }
     * k5: { ?o age ?p }
     * k6: { ?h a Person ; label ?i . ?a ?b ?c . ?x ?y ?z }
     *
     *
     * Another possible issue: Disconnected sub-sets. Any iso we find for ?a ?b ?c
     * does not 'constrain' the other patterns. So it may clutter up the index because it may well become
     * a leaf of many nodes in the tree. Not sure if we can do better by a higher level index that
     * splits up the sets and indexes them individually.
     *
     *
     *   / k1: { ?a a Person } - k3: { ?s label ?o } \
     * {}                                              ??? k3 -> k4 ???
     *   \ k2: { ?b label ?c } - k3: { ?s a Person } /
     *
     * I am now unsure about whether we can optimize something due to diamond patterns:
     *
     * k3: { ?s label ?o } means:
     * If some query residual graph q is isomorphic to { ?s label ?o } it is a match for k3
     *
     * But then, whenever for a new graph e.g. k4 it turns out there is an iso from another
     * graph k3, won't k4 then become a child to every node corresponding to k3?
     * But then, we can just record: k3 -> k4.
     * Yet, k4 would also be a child of k5
     *
     *
     * So the change would be, that in the index, keys become nodes,
     * and the residual graph becomes part of the edge
     * Graph<K, Edge>
     * Edge: {
     *   // "By going from fromKey and mapping via transIso we reach toKey; with residualGraph remaining
     *   K fromKey;
     *   BiMap<N, N> transIso;
     *   K toKey;
     *   Graph residualGraph;
     * }
     *
     *
     * Note: residual graphs could be isomorphic to each other
     *
     *
     * @param key
     * @param pos
     * @param forceInsert
     * @param writer
     */
//    int foobar = 0;
    void performAdd(K key, InsertPosition<K, G, V, T> pos, boolean forceInsert) { //, IndentedWriter writer) {
//    	if(("" + key).equals("http://lsq.aksw.org/res/q-00ea1cb7=0")) {
//    		++ foobar;
//    		
//    		if(foobar == 12) {
//    			System.out.println("got the case where it goes wrong");
//    		}
//    		
//    		System.out.println("Got key: " + foobar + " - " + key);
//	    	System.out.println();
//	    	System.out.println("BEFORE");
//	    	printTree();
//	    	performAddCore(key, pos, forceInsert);
//	    	System.out.println();
//	    	System.out.println("AFTER");
//	    	printTree();
//	    	System.out.println();
//    	} else {
        	performAddCore(key, pos, forceInsert);
//    	}
    }
    
    void performAddCore(K key, InsertPosition<K, G, V, T> pos, boolean forceInsert) { //, IndentedWriter writer) {
    	
//    	System.out.println("Performing add of " + key);
//    	System.out.println("  baseIso: " + pos.getIso());
//    	System.out.println("  latestIso: " + pos.getLatestIsoAB());
    	
    	IndexNode<K, G, V, T> nodeA = pos.getNode();
        //Graph insertGraphIsoB = pos.getGraphIso();

        G residualInsertGraphB = pos.getResidualQueryGraph();
        Set<T> residualInsertGraphBTags = pos.getResidualQueryGraphTags();


        // If the insert graph is empty, just append the key to the insert node
        // i.e. do not create a child node
        // In this case,
        if(setOps.isEmpty(residualInsertGraphB)) {
            K prefKey = nodeA.getKey();
            Map<K, Set<BiMap<V, V>>> altKeyToIso = prefKeyToAltKeysWithIso.row(prefKey);

            BiMap<V, V> deltaIso = removeIdentity(pos.getIso());
            altKeyToIso.computeIfAbsent(key, (k) -> new LinkedHashSet<>()).add(deltaIso);

            return;
        }

        BiMap<V, V> transIsoAB = pos.getLatestIsoAB();
        BiMap<V, V> nodeBaseIso = pos.getNodeBaseIso();
        BiMap<V, V> baseIsoAB = pos.getIso();
        
        BiMap<V, V> baseIsoBA = baseIsoAB.inverse();



        // TODO isoAB should have an empty intersection with iso parentOfAtoA
        // Make a sanity check to ensure that

        // If nodeA does not have children, we just append nodeB
        // Otherwise, we need to do a more complex update procedure where we
        // re-insert every graph in the node's subtree
        
        IndexNode<K, G, V, T> nodeB = keyToNode.get(key);
        
        if(nodeB == null) {
            nodeB = createNode(key, null, null); //residualInsertGraphB, residualInsertGraphBTags);
            //K prefKey = key; //getPrefKey(key);
            Map<K, Set<BiMap<V, V>>> altKeyToIso = prefKeyToAltKeysWithIso.row(key);
            // We can avoid creating a new map if the key is already an entry
            altKeyToIso.computeIfAbsent(key, k -> new LinkedHashSet<>(Collections.singleton(HashBiMap.<V, V>create())));

        }
        
        //if(nodeA.isLeaf() || forceInsert) { //nodeA.childIndex.isEmpty()) {
if(false) {
//            if(("" + nodeA.getKey()).equals("http://lsq.aksw.org/res/q-00d5ab86=2") && ("" + nodeB.getKey()).equals("http://lsq.aksw.org/res/q-00ea1cb7=0")) {
//                System.out.println("got critital case");
//            }
//            if(Objects.equals(nodeA.getKey(), nodeB.getKey())) {
//            	throw new RuntimeException("");
//            }
        	
            nodeA.appendChild(nodeB, residualInsertGraphB, residualInsertGraphBTags, transIsoAB, baseIsoAB);
            K prefKey = getPrefKey(key);
            //Map<K, BiMap<V, V>> altKeyToIso = prefKeyToAltKeysWithIso.row(prefKey);
            // We can avoid creating a new map if the key is already an entry
            //altKeyToIso.computeIfAbsent(key, k -> HashBiMap.create());

        } else {

        	// There is 2 things to do now:
        	// (1) Update all direct edges of nodeA, and check whether they need to go to nodeB instead
        	// (2) Create all edges for nodeB

//        	System.out.println("Inner insert: " + key);
        	
        	// candEdges are edges leading to graphs that might subsume the inserted graph
        	// i.e. the inserted graph may become a sub-graph of any of those
            Set<Edge<K, G, V, T>> directCandEdges =
                    nodeA.getEdgeIndex().getAllSupersetsOf(residualInsertGraphBTags, false).keySet();


        	//directCandEdges.forEach(nodeA.getEdgeIndex()::remove);

            // nonCandEdges lead to graphs that definitely do NOT subsume the inserted graph
//            List<Edge<K, G, V, T>> directNonCandEdges =
//            		nodeA.getEdgeIndex().keySet().stream()
//                    //nodeA.getTargetKeyToEdges().values().stream()
//                    .filter(edge -> !directCandEdges.contains(edge))
//                    .collect(Collectors.toList());
	        
	        // Remove graphs of nodeA.key and key (avoids possibly matching graphs to themselves)
//	        graphKeyToGraphAndTags.remove(nodeA.getKey());
//	        graphKeyToGraphAndTags.remove(key);
	        
	        
	        // We need to replay the insert of all reachable graphs
//	        List<Entry<K, Entry<G, Set<T>>>> graphsBySize = new ArrayList<>(graphKeyToGraphAndTags.entrySet());
//	        Collections.sort(graphsBySize, (a, b) -> setOps.size(a.getValue().getKey()) - setOps.size(b.getValue().getKey()));

	                    
            // If the inserted node's residual graph is non-empty, we can draw an edge to its ID
            
            // Append the child node and associate it with the key
            nodeA.appendChild(nodeB, residualInsertGraphB, residualInsertGraphBTags, transIsoAB, baseIsoAB);

            

            
        	G viewGraph = residualInsertGraphB;
        	Set<T> viewGraphTags = residualInsertGraphBTags;
        	

            // We have to go through all candidate edges and check which of them now have
            // to pass through key to their original target
            
            // Note, that 'key' becomes a child of this node in any case - the only question is whether
            // there are isomorphisms to the edges' targets
            for(Edge<K, G, V, T> edgeAC : directCandEdges) {
            	
            	K edgeTargetKey = edgeAC.getTo();
            	
            	if(Objects.equals(key, edgeTargetKey)) {
            		// Skip edges leading to edgeTargetKey itself
            		// May happen if the same key under a different isomorphism is a child of nodeA
            		continue;
            	}
            	
            	G insertGraph = edgeAC.getResidualGraph();
            	Set<T> insertGraphTags = edgeAC.getResidualGraphTags();
                BiMap<V, V> transIsoAC = edgeAC.getTransIso();
//                BiMap<V, V> transBaseIso = mapDomainVia(baseIsoAB, transIsoAC);

        	    //BiMap<V, V> baseIsoBC = chain(baseIsoBA, transIsoAC);     

        	    BiMap<V, V> baseIsoBC = mapRangeVia(baseIsoBA, transIsoAC);     

        	    //BiMap<V, V> altBaseIsoBC = mapRangeVia(baseIsoAB, transIsoBC);

//        	    System.out.println("Alt baseIsoBC: " + baseIsoBC);
        	    
            	Iterable<BiMap<V, V>> isosBC = isoMatcher.match(baseIsoBC, viewGraph, insertGraph);
            	
            	boolean isSubsumed = false;
            	for(BiMap<V, V> isoBC : isosBC) {
//            		System.out.println("Found iso: " + iso);
            		BiMap<V, V> deltaIsoBC = removeIdentity(isoBC);
                    boolean isCompatible = MapUtils.isCompatible(isoBC, baseIsoBC);
                    if(!isCompatible) {
                    	continue;
                    }
                    
            		isSubsumed = true;
            		
                    G g = setOps.applyIso(viewGraph, deltaIsoBC);

                    G residualInsertGraph = setOps.difference(insertGraph, g);
                    Set<T> residualInsertGraphTags = Sets.difference(insertGraphTags, viewGraphTags);
                    // now create the diff between the insert graph and mapped child graph
//                    writer.println("Diff " + residualInsertGraph + " has "+ setOps.size(residualInsertGraph) + " triples at depth " + writer.getUnitIndent());

                    if(setOps.isEmpty(residualInsertGraph)) {
                    	System.out.println("graph is empty 1");
                    }
                    
                    // TODO optimize handling of empty diffs
            		IndexNode<K, G, V, T> targetNode = keyToNode.get(edgeTargetKey);
                    
            		nodeB.appendChild(targetNode, residualInsertGraph, residualInsertGraphTags, deltaIsoBC, null);
	                //add(nodeB, edgeTargetKey, residualInsertGraph, residualInsertGraphTags, null, transBaseIso, deltaIso, false);

            	}
            	
            	// Remove the edge from the edge index
            	if(isSubsumed) {
            		//System.out.println("BEFORE: " + nodeA.getEdgeIndex().size());
            		
            		
            		int sizeBefore = nodeA.getEdgeIndex().size();
//            		nodeA.getEdgeIndex().remove(edgeAC);
            		nodeA.removeEdge(edgeAC);
            		int sizeAfter = nodeA.getEdgeIndex().size();
            		
            		int d = sizeBefore - sizeAfter;
            		if(!(d == 1)) {
            			throw new AssertionError("Identity-based deletion failed");
            		}
            	
            		//System.out.println("AFTER: " + nodeA.getEdgeIndex().size());
            		//System.out.println();
            	}
            }

            
//            System.out.println("Tree after breaking edges:");
//            printTree();
//if(true) return;
        	// Starting from nodeB:
        	// (1) Find all reachable graphs together with all isos
        	// (2) Compute isos between the insertGraph and all reachable graphs
        	//     All isos not covered by (1) become immediate edges of nodeB
        	
            // All graphs reachable via directCandEdges need to be rechecked for isos
        	
        	//Set<Edge<K, G, V, T>> nodeBEdges = nodeB.edgeIndex.keySet();
            //Set<Edge<K, G, V, T>> nodeBEdges = directCandEdges;
            Set<Edge<K, G, V, T>> nodeBEdges = nodeA.edgeIndex.keySet();
            if(false) {
//        	if(!nodeBEdges.isEmpty()) {
//        		System.out.println("Non empty edges on nodeB");
//        	}
        	
            // TODO: Filter the graphs by those whose accumulated residual graph tags are a superset of those of the insert graph
            Map<K, Set<T>> reachableGraphKeyToTags = loadReachableGraphTags(new LinkedHashMap<>(), Collections.emptySet(), nodeBEdges);
            
            
            // Index the graph keys by the tags
            TagMap<K, T> reachableGraphIndex = new TagMapSetTrie<>(tagComparator);
            for(Entry<K, Set<T>> entry : reachableGraphKeyToTags.entrySet()) {
                reachableGraphIndex.put(entry.getKey(), entry.getValue());
            }
            
            // Find all graphs whose tags are a super set of those being inserted
            Set<K> candSuperGraphKeys = reachableGraphIndex.getAllSupersetsOf(residualInsertGraphBTags, false).keySet();
            
            // Only load the cand graphs
            Map<K, Entry<G, Set<T>>> reachableGraphKeyToGraphAndTags = loadGraphsInSubTreesCore(new LinkedHashMap<>(), setOps.createNew(), Collections.emptySet(), nodeBEdges);
            }
            
                        
            G graphA = keyToGraph.get(key);
            Set<T> allViewGraphTags = graphsByTags.get(key);
            
            Set<K> candSuperGraphKeys = graphsByTags.getAllSupersetsOf(allViewGraphTags, false).keySet();
            
            
            //System.out.println("Collected " + reachableGraphKeyToGraphAndTags.keySet().size() + " residual graphs");
            //System.out.println("Relevant graphs: " + candSuperGraphKeys.size());
            
            
            
            
            //candSuperGraphKeys.remove(nodeA.getKey());
            //candSuperGraphKeys.remove(key);
            
            
	        //Map<K, Multimap<Edge<K, G, V, T>, BiMap<V, V>>> graphKeyToReachingEdgeAndIso = loadIsoReachableGraphKeysOld(directCandEdges);

            
            //Multimap<K, BiMap<V, V>> reachableGraphKeyToIsos = loadIsoReachableGraphKeys2(nodeBEdges);

            
            //System.out.println("Dealing with " + reachableGraphKeyToIsos.size() + " known isos");
            
            for(K insertGraphKey : candSuperGraphKeys) {                
	        //for(Entry<K, Entry<G, Set<T>>> insertGraphEntry : reachableGraphKeyToGraphAndTags.entrySet()) {
	        	            	
	        	if(Objects.equals(insertGraphKey, key)) {
	        		continue;
	        	}


	        	G graphB = keyToGraph.get(insertGraphKey);
	        	G graphAInB;
	        	try {
	        		graphAInB = setOps.applyIso(graphA, baseIsoAB);
	        	} catch(Exception e) {
	        		// HACK (or rather hacky): if this fails, we simply assume that graphA is not a sub-graph of graphB
	        		continue;
	        		//logger.debug("");
	        	}
	        	
	        	G diff = setOps.difference(graphAInB, graphB);
	        	boolean isASubgraphOfB = setOps.isEmpty(diff);
	        	
	        	if(!isASubgraphOfB) {
	        		continue;
	        	}

	        	G insertGraph = setOps.difference(graphB, graphAInB);

	        	
	        	
	        	
	        	// Note: We expect a set multimap here
	        	//Set<BiMap<V, V>> knownIsosAC = (Set<BiMap<V, V>>)reachableGraphKeyToIsos.get(insertGraphKey);
	        	
	        	// For small graphs its certainly cheaper to just recompute the isos rather than traversing the whole index tree to find
	        	// all known isos
	        	
	        	Set<BiMap<V, V>> knownIsosAC = new HashSet<>();
	        	G nodeGraph = keyToGraph.get(pos.getNode().getKey());
	        	Iterable<BiMap<V, V>> isosAC = isoMatcher.match(HashBiMap.create(), nodeGraph, insertGraph);
	        	
	        	for(BiMap<V, V> isoAC : isosAC) {
	        		knownIsosAC.add(isoAC);
	        	}
	        	
	        	
            	//System.out.println("  Known isos for " + insertGraphKey + ": " + knownIsosAC.size());

         		//IndexNode<K, G, V, T> targetNode = keyToNode.get(insertGraphKey);

	        	
	        	//Entry<G, Set<T>> tmp = insertGraphEntry.getValue();
	        	//G insertGraph = reachableGraphKeyToGraphAndTags.get(insertGraphKey).getKey();
	        	//Set<T> insertGraphTags = reachableGraphKeyToTags.get(insertGraphKey);
            	Set<T> allInsertGraphTags = graphsByTags.get(insertGraphKey);
            	
	        	
	        	// Approach A: For each know iso, invoke the isoMatcher on the residual graph
	        	//   Approach has been thoroughly tested and most likely bug free
	        	// Approach B: Invoke the isoMatcher first, and check against known isos
	        	//             (this should significantly reduce invocations of the iso matcher)
	        	boolean approachA = true;
	        	
	        	if(approachA) {
	        	
	        		
	//                BiMap<V, V> transBaseIso = mapDomainVia(baseIsoAB, transIsoAB);
	//        		
		        	for(BiMap<V, V> knownIsoAC : knownIsosAC) {
			        	
		        	    BiMap<V, V> baseIsoBC;
		        	    try {
		        	        baseIsoBC = mapRangeVia(baseIsoBA, knownIsoAC);
		        	    } catch(Exception e) {
		        	        logger.warn("Not sure if we can safely ignore this case", e);
		        	        continue;
		        	    }
	
		        		
		        	    // Note: the computed isos are just based on the residual graphs
		        	    // thus, they are not complete for B, nor are they a minimal delta (i.e. identities removed)
		            	Iterable<BiMap<V, V>> residualIsosBC = isoMatcher.match(baseIsoBC, viewGraph, insertGraph);
		
		            	//System.out.println("  Found " + Iterables.size(residualIsosBC) + " candidate isos");
		            	
		            	for(BiMap<V, V> residualIsoBC : residualIsosBC) {
		                    boolean isCompatible = MapUtils.isCompatible(residualIsoBC, baseIsoBC);
		                    if(!isCompatible) {
		                    	continue;
		                    }
		 
		            		
		                    BiMap<V, V> deltaIsoBC = removeIdentity(residualIsoBC);
		                    BiMap<V, V> deltaIsoAC = removeIdentity(mapRangeVia(baseIsoAB, deltaIsoBC));
		                    //BiMap<V, V> deltaIsoAC = removeIdentity(mapDomainVia(deltaIsoBC, baseIsoBA));
		
		                    
		                    // Skip known isos
		                    ////boolean isKnown = knownIsos.contains(deltaIso);
		                    
		                    boolean isKnown = knownIsosAC.contains(deltaIsoAC);
		                    
		                    if(isKnown) { 
		//                    	System.out.println("Known iso: " + insertGraphKey + ": " + deltaIso);
		                    	continue;
		                    }
	
	//	                    
	//	                    boolean testA = knownIsosAC.stream().anyMatch(knowIsoAC -> knowIsoAC.entrySet().containsAll(deltaIsoAC.entrySet()));
	//	                    if(testA) {
	//	                    	System.out.println("SUBSUMED ISO 1");
	//	                    }
	//	                    
	//	                    Set<BiMap<V, V>> testB = knownIsosAC.stream().filter(knowIsoAC -> deltaIsoAC.entrySet().containsAll(knowIsoAC.entrySet())).collect(Collectors.toSet());
	//	                    if(!testB.isEmpty()) {
	//	                    	System.out.println("SUBSUMED ISO 2: deltaIsoAC " + deltaIsoAC + " contained:");
	//	                    	System.out.println("from  " + key + " to " + insertGraphKey + " via " + removeIdentity(baseIsoBC));
	//	                    	System.out.println("Full base IsoBC: " + baseIsoBC);
	//	                    	System.out.println(testB);
	//	                    }
	//	                    
		                    
		                    G g = setOps.applyIso(viewGraph, deltaIsoBC);
		
		                    G residualInsertGraph = setOps.difference(insertGraph, g);
		                    Set<T> residualInsertGraphTags = Sets.difference(allInsertGraphTags, allViewGraphTags);
		                    // now create the diff between the insert graph and mapped child graph
		// 	                    writer.println("Diff " + residualInsertGraph + " has "+ setOps.size(residualInsertGraph) + " triples at depth " + writer.getUnitIndent());
		
		                    IndexNode<K, G, V, T> targetNode = keyToNode.get(insertGraphKey);
		            		
		//                    System.out.println("Addending transitive edge to " + insertGraphKey);
		                    
		                    if(setOps.isEmpty(residualInsertGraph)) {
		                    	System.out.println("graph is empty 2");
		                    }
		
		                    nodeB.appendChild(targetNode, residualInsertGraph, residualInsertGraphTags, deltaIsoBC, null);
		            		
		//                    System.out.println();
		                    //add(nodeB, insertGraphKey, residualInsertGraph, residualInsertGraphTags, null, baseIsoAB, deltaIso, false);
		            	}	
		        	}
	        	} else {
//	        		
//	        		// Not working - probably forget about this case!!!
//	        		
//	            	Iterable<BiMap<V, V>> residualIsosBC = isoMatcher.match(HashBiMap.create(), viewGraph, insertGraph);
//	        		
//	            	//System.out.println("  Found " + Iterables.size(residualIsosBC) + " candidate isos");
//
//	            	for(BiMap<V, V> residualIsoBC : residualIsosBC) {
//
//	                    BiMap<V, V> deltaIsoBC = removeIdentity(residualIsoBC);
//
//	            		// The residual mapping BC has to be compatible with baseIsoAB:
//	            		BiMap<V, V> baseIsoAC;
//	            		try {
//	            			baseIsoAC = mapRangeVia(baseIsoAB, deltaIsoBC);
//	            		} catch(Exception e) {
//	            			continue;
//	            		}	            		
//
//	            		// AC and AB have to be compatible
//	            		boolean isCompatible = MapUtils.isCompatible(baseIsoAB, baseIsoAC);
//	            		if(!isCompatible) {
//	            			continue;
//	            		}
//	            		
//	                    BiMap<V, V> deltaIsoAC = removeIdentity(baseIsoAC);
//
//	                    boolean isKnown = knownIsosAC.contains(deltaIsoAC);
//	                    if(!isKnown) {
//		            		
//		                    G g = setOps.applyIso(viewGraph, deltaIsoBC);
//		
//		                    G residualInsertGraph = setOps.difference(insertGraph, g);
//		                    Set<T> residualInsertGraphTags = Sets.difference(insertGraphTags, viewGraphTags);
//		                    // now create the diff between the insert graph and mapped child graph
//		// 	                    writer.println("Diff " + residualInsertGraph + " has "+ setOps.size(residualInsertGraph) + " triples at depth " + writer.getUnitIndent());
//		
//		                    IndexNode<K, G, V, T> targetNode = keyToNode.get(insertGraphKey);
//		            		
//		//                    System.out.println("Addending transitive edge to " + insertGraphKey);
//		                    
//		                    if(setOps.isEmpty(residualInsertGraph)) {
//		                    	System.out.println("graph is empty 2");
//		                    }
//		
//		                    nodeB.appendChild(targetNode, residualInsertGraph, residualInsertGraphTags, deltaIsoBC, null);
//		            		
//		//                    System.out.println();
//		                    //add(nodeB, insertGraphKey, residualInsertGraph, residualInsertGraphTags, null, baseIsoAB, deltaIso, false);
//		            	}	
//	            	}
		        		
	        	}
	        	
	        	
	        	
	        }
            
            
            
            
            
            
            
            
            
            
            
	        
//	        System.out.println("Updated tree:");
//	        printTree();
////	        	
//	 	            for(Entry<Edge<K, G, V, T>, Collection<BiMap<V, V>>> edgeToTransIsos : graphKeyToReachingEdgeAndIso.get(targetGraphKey).asMap().entrySet()) {
//	 	                Edge<K, G, V, T> edge = edgeToTransIsos.getKey();
//	 	                
//	 	                BiMap<V, V> baseIsoAC = edge.getBaseIso();
//	 	             
//	 	                for(BiMap<V, V> transIso : edgeToTransIsos.getValue()) {          
//		 	            	
//		 	            	//K edgeTargetKey = edge.getTo();
//		 	            	//G insertGraph = edge.getResidualGraph();
//		 	            	//Set<T> insertGraphTags = edge.getResidualGraphTags();
//		 	                BiMap<V, V> transBaseIso = mapDomainVia(baseIsoAC /* baseIsoAB ??? */, transIso);
//		
//		 	            	Iterable<BiMap<V, V>> isos = isoMatcher.match(transBaseIso, viewGraph, insertGraph);
//		
//		 	            	for(BiMap<V, V> iso : isos) {
//		 	                    BiMap<V, V> deltaIso = removeIdentity(iso);
//	
//		 	                    G g = setOps.applyIso(viewGraph, deltaIso);
//		
//		 	                    G residualInsertGraph = setOps.difference(insertGraph, g);
//		 	                    Set<T> residualInsertGraphTags = Sets.difference(insertGraphTags, viewGraphTags);
//		 	                    // now create the diff between the insert graph and mapped child graph
//		// 	                    writer.println("Diff " + residualInsertGraph + " has "+ setOps.size(residualInsertGraph) + " triples at depth " + writer.getUnitIndent());
//		
//		 	            		//nodeB.appendChild(targetNode, residualInsertGraph, residualInsertGraphTags, iso, transBaseIso);
//		 	            		
//		 	                    add(nodeB, targetGraphKey, residualInsertGraph, residualInsertGraphTags, null, baseIsoAC, deltaIso, true);
//		 	            	}	 	            	
//		 	            }
//	 	            }
//	        }
//            
//            
            //            
            
            // FIXME Maybe its like this:
            // - We need to flatten descendant graphs so we can determine whether they subsume B (the inserted graph)
            // - For each descendant graph collect all isomorphisms that reached them from node A
            
            
            // Find all residual graphs reachable from this node and order them by size


            // Append all unaffected non-candidate children
//            for(Edge<K, G, V, T> edge : directNonCandEdges) {
//                K targetNodeKey = edge.getTo();
//                IndexNode<K, G, V, T> targetNode = keyToNode.get(targetNodeKey);
//                nodeA.appendChild(targetNode, edge.getResidualGraph(), edge.getResidualGraphTags(), edge.getTransIso(), edge.getBaseIso());
//            }
        }
    }
    
}



// Lots of useless code below; remove at some point


//
//Map<K, Entry<G, Set<T>>> keyToGraphAndTags = loadGraphsInSubTreesCore(new LinkedHashMap<>(), setOps.createNew(), Collections.emptySet(), directCandEdges);
//// For every isomorphism the (keys of the) reached graphs
//
//
//
////Multimap<K, Edge<K, G, V, T>> isoToReachableGraphKeys = loadIsoReachableGraphKeys(ArrayListMultimap.create(), null, directCandEdges);
//Map<K, Multimap<Edge<K, G, V, T>, BiMap<V, V>>> isoToReachableGraphKeys = loadIsoReachableGraphKeysOld(directCandEdges);
//
//
//keyToGraphAndTags.put(key, new SimpleEntry<>(residualInsertGraphB, residualInsertGraphBTags));
//isoToReachableGraphKeys.computeIfAbsent(key, (k) -> newSetMultimap(true, false))
//.put(new Edge<>(nodeA.getKey(), key, transIsoAB, residualInsertGraphB, residualInsertGraphBTags, baseIsoAB), transIsoAB);
//
////Map<K, Entry<BiMap<V, V>, Entry<G, Set<T>>>> keyToGraphAndTags = loadGraphsInSubTrees(setOps.createNew(), Collections.emptySet(), directCandEdges);
//
//
////System.out.println("Found these graphs in subtree of candidate children: " + keyToGraph.keySet());
//
////List<Entry<K, Entry<BiMap<V, V>, Entry<G, Set<T>>>>> keyGraphs = new ArrayList<>(keyToGraphAndTags.entrySet());
////Collections.sort(keyGraphs, (a, b) -> setOps.size(a.getValue().getValue().getKey()) - setOps.size(b.getValue().getValue().getKey()));
//
//List<Entry<K, Entry<G, Set<T>>>> keyGraphs = new ArrayList<>(keyToGraphAndTags.entrySet());
//Collections.sort(keyGraphs, (a, b) -> setOps.size(a.getValue().getKey()) - setOps.size(b.getValue().getKey()));
//
//// Remove all edges reachable via directCandEdges - i.e. clear data of all reachable nodes
//Set<IndexNode<K, G, V, T>> reachableNodes = directCandEdges.stream()
//    .map(Edge::getTo)
//    .map(keyToNode::get)
//    .flatMap(node -> reachableNodes(node,
//    		n -> n.getEdgeIndex().keySet().stream().map(Edge::getTo).map(keyToNode::get)))
//            //n -> n.getTargetKeyToEdges().keySet().stream().map(keyToNode::get)))
//    .collect(Collectors.toCollection(Sets::newIdentityHashSet));
//
//reachableNodes.forEach(node -> {
//    node.clearLinks(true);
//});
//
//// Create the node that will replace nodeA, and
//// create and append the insert node as a child
////TagMap<Edge<K, G, V, T>, T> tagMap = new TagMapSetTrie<>(tagComparator);
////IndexNode<K, G, V, T> replacementNodeA = new IndexNode<>(nodeA.getKey(), nodeA.getGraph(), nodeA.getGraphTags(), tagMap);
//
//// Clear nodeA and append the insert data as the only child
//nodeA.clearLinks(false);
//
//// TODO Lets make this graph part of the ordinary insert procedure
////nodeA.appendChild(nodeB, residualInsertGraphB, residualInsertGraphBTags, transIsoAB, baseIsoAB);
//
//// Add the key
//Map<K, BiMap<V, V>> altKeyToIso = prefKeyToAltKeysWithIso.row(key);
//altKeyToIso.put(key, HashBiMap.create());
//
//
//// Perform additions
//for(Entry<K, Entry<G, Set<T>>> keyGraph : keyGraphs) {
//	
////for(Entry<K, Entry<BiMap<V, V>, Entry<G, Set<T>>>> keyGraph : keyGraphs) {
//    K graphKey = keyGraph.getKey();
//
//    
////    if(graphKey.equals(key)) {
////    	System.out.println("Skipping same insert: " + graphKey);
////    	continue;
////    }
//    
//    if(("" + graphKey).equals("http://lsq.aksw.org/res/q-00dc64d6=0")) {
//        System.out.println("HERE");
//    }
//
//    
//    //Entry<BiMap<V, V>, Entry<G, Set<T>>> tmp = keyGraph.getValue();
//    //BiMap<V, V> transIso = tmp.getKey();
//    //Entry<G, Set<T>> graphAndTags = tmp.getValue();
//    Entry<G, Set<T>> graphAndTags = keyGraph.getValue();
//    
//    G residualChildGraph = graphAndTags.getKey();
//    Set<T> residualChildGraphTags = graphAndTags.getValue(); 
//    
//    //System.out.println("Isos for graphKey " + graphKey + " with nodeBaseIso " + nodeBaseIso);
//    //for(Edge<K, G, V, T> edgeAC : isoToReachableGraphKeys.get(graphKey)) {
//    for(Entry<Edge<K, G, V, T>, Collection<BiMap<V, V>>> edgeToTransIsos : isoToReachableGraphKeys.get(graphKey).asMap().entrySet()) {
//        Edge<K, G, V, T> edge = edgeToTransIsos.getKey();
//        BiMap<V, V> baseIsoAC = edge.getBaseIso();
//        
////        for(Entry<BiMap<V, V>, Set<T>> transIsoAndTags : edgeToTransIsos.getValue()) {                      
////            BiMap<V, V> transIsoAC = transIsoAndTags.getKey();
////            Set<T> residualChildGraphTagsX = transIsoAndTags.getValue();
//        for(BiMap<V, V> transIsoAC : edgeToTransIsos.getValue()) {                      
//            
////            if(("" + graphKey).equals("http://lsq.aksw.org/res/q-00e5a47a=0")) {
////
////                if(("" + residualChildGraph).contains("?a")) {
////                    System.out.println("got ?a");
////                }
////                
////                
////                System.out.println("baseIsoAC: " + baseIsoAC + "transIsoAC: " + transIsoAC);
////                
////            }
//            
//        	//transIsoAC = HashBiMap.create();
//            add(nodeA, graphKey, residualChildGraph, residualChildGraphTags, null, baseIsoAC, transIsoAC, true);                
//        }
//        
//        
////    	System.out.println("  Iso: " + transIsoAC);
////
////    	if(!transIsoAC.isEmpty()) {
////    		System.out.println("    non empty!");
////    	}
//    	
//    	//                  BiMap<V, V> transIsoAC = nodeC.getTransIso();
////      BiMap<V, V> transBaseIsoBC = chain(baseIsoAB.inverse(), transIsoAC);
////      //mapDomainVia(nodeC.getTransIso(), );
//    	
//    	// We do not know whether B is a subgraph of C under isomorphism
//    	// We know that
//    	// - A is a subgraph of both B and C
//    	// - There is the newly found iso to B (baseIso, latestIsoAB) 
//    	// - There are the prior isos (=edges) from A to C
//    	// 
//    	
//    	//BiMap<V, V> baseIsoBC = 
//    	
////    	System.out.println("transIsoAB: " + transIsoAB);
////    	System.out.println("transIsoAC: " + transIsoAC);
////    	System.out.println("transIsoBC: " + transIsoBC);
////    	System.out.println("baseIsoAB:" + baseIsoAB);
//    	//System.out.println("baseIsoAC:" + baseIsoAC);
//    	
//    	//transIsoAB
//    	//transIsoAC.forEach((k, v) -> baseIsoAC.remove(k, v));
////    	
////    	BiMap<V, V> baseIsoAC = HashBiMap.create(edgeAC.getBaseIso());
////    	//BiMap<V, V> transIsoAC = edgeAC.getTransIso();
////    	
////    	System.out.println("baseIsoAB: " + baseIsoAB);
////    	System.out.println("transIsoAB: " + transIsoAB);
////    	System.out.println("baseIsoAC: " + baseIsoAC);
////    	System.out.println("transIsoAC:" + transIsoAC);
////    	//System.out.println("baseIsoAC:" + baseIsoAC);
////    	
////    	BiMap<V, V> transIsoBC = chain(transIsoAB.inverse(), transIsoAC);                	
////    	BiMap<V, V> altBaseIsoAC = mapRangeVia(baseIsoAB, transIsoBC);
////
////    	System.out.println("transIsoBC" + transIsoBC);
////    	System.out.println("altBaseIsoAC" + altBaseIsoAC);
////    	if(!altBaseIsoAC.equals(transIsoAC)) {
//////    		throw new RuntimeException("Difference");
////    		System.out.println("Different!");
////    	} else {
////    		System.out.println("Equivalent!");
////    	}
////    	
////    	if(!baseIsoAC.isEmpty()) {
////    		System.out.println("got a non empty baseIsoAC");
////    	}
////        if(("" + graphKey).equals("http://lsq.aksw.org/res/q-00ea1cb7=0")) {
////            String xxx = "" + baseIsoAC;
////            String yyy = "" + transIsoAC;
////            
////            if(xxx.contains("instance") && yyy.contains("instance")) {
////                ++arghCount;
////                System.out.println("argh: " + arghCount);
////                
////                if(arghCount == 17) {
////                    System.out.println("Got the case");
////                }
////            }
////            
////        	System.out.println("Here");
////        }
//        
////        add(nodeA, graphKey, residualChildGraph, residualChildGraphTags, null, baseIsoAC, transIsoAC, true);                
////    
////    
////        printTree();
////        System.out.println("Done printing tree");
//    }                
//    //Set<T> insertGraphTags = extractGraphTagsWrapper(insertGraph);
//    //BiMap<V, V> baseIso = HashBiMap.create();
//
//    // x add(nodeA, k, insertGraph, insertGraphTags, baseIsoAB, true); //, writer);
//    //add(nodeA, k, insertGraph, insertGraphTags, baseIsoAB, transIso, true);
//    
//}
//

/**
 * This method is based on a conceptual error.
 * When inserting new graph at a node, we need to replay the
 * insert of all children; rather then just checking whether the newly
 * inserted graph is a sub-graph iso of a direct child.
 *
 * @param key
 * @param pos
 * @param writer
 */
//void performAddBullshit(K key, InsertPosition<K, G, V, T> pos, IndentedWriter writer) {
//    GraphIndexNode<K, G, V, T> nodeA = pos.getNode();
//    //Graph insertGraphIsoB = pos.getGraphIso();
//
//    G residualInsertGraphB = pos.getResidualQueryGraph();
//    Set<T> residualInsertGraphBTags = pos.getResidualQueryGraphTags();
//
//
//    // If the insert graph is empty, just append the key to the insert node
//    // i.e. do not create a child node
//    if(isEmpty(residualInsertGraphB)) {
//        nodeA.getKeys().add(key);
//        return;
//    }
//
//
//
//    BiMap<V, V> isoAB = pos.getLatestIsoAB();
//    BiMap<V, V> baseIsoAB = pos.getIso();
//
//    // TODO isoAB should have an empty intersection with iso parentOfAtoA
//    // Make a sanity check to ensure that
//
//    GraphIndexNode<K, G, V, T> nodeB = createNode(residualInsertGraphB, residualInsertGraphBTags, isoAB);
//    nodeB.getKeys().add(key);
//
//    writer.println("Insert attempt of user graph of size " + setOps.size(residualInsertGraphB));
////    RDFDataMgr.write(System.out, insertGraph, RDFFormat.NTRIPLES);
////    System.out.println("under: " + currentIso);
//
//    // If the addition is not on a leaf node, check if we subsume anything
//    boolean isSubsumed = nodeA.getChildren().stream().filter(c -> !c.getKeys().contains(key)).count() == 0;//;isEmpty(); //{false};
//
//
//    // TODO We must not insert to nodes where we just inserted
//
//    // Make a copy of the baseIso, as it is transient due to state space search
//    //GraphIsoMap gim = new GraphIsoMapImpl(insertGraph, HashBiMap.create(baseIso));
//
//    //boolean wasAdded = false;
//
//    // If the insertGraph was not subsumed,
//    // check if it subsumes any of the other children
//    // for example { ?s ?p ?o } may not be subsumed by an existing child, but it will subsume any other children
//    // use clusters
//    // add it as a new child
//    if(!isSubsumed) {
//        writer.println("We are not subsumed, but maybe we subsume");
////        GraphIndexNode<K> nodeB = null;//createNode(graphIso);//new GraphIndexNode<K>(graphIso);
//
//
//        writer.incIndent();
//        //for(GraphIndexNode child : children) {
//        //Iterator<GraphIndexNode<K>> it = nodeA.getChildren().iterator();//children.listIterator();
//        Iterator<GraphIndexNode<K, G, V, T>> it = new ArrayList<>(nodeA.getChildren()).iterator();
//        while(it.hasNext()) {
//            GraphIndexNode<K, G, V, T> nodeC = it.next();
//            G viewGraphC = nodeC.getValue();
//            Set<T> viewGraphCTags = nodeC.getGraphTags();
//
//            writer.println("Comparison with view graph of size " + setOps.size(viewGraphC));
////            RDFDataMgr.write(System.out, viewGraph, RDFFormat.NTRIPLES);
////            System.out.println("under: " + currentIso);
//
//            // For every found isomorphism, check all children whether they are also isomorphic.
//            writer.incIndent();
//            int i = 0;
//
//            boolean isSubsumedC = false;
////baseIso: ?x -> ?y, transIso: ?x -> ?z => ?y -> ?z
//            BiMap<V, V> transIsoAC = nodeC.getTransIso();
//            BiMap<V, V> transBaseIsoBC = chain(baseIsoAB.inverse(), transIsoAC);
//            //mapDomainVia(nodeC.getTransIso(), );
//
//
//            //Iterable<BiMap<V, V>> isosBC = isoMatcher.match(baseIso.inverse(), residualInsertGraphB, viewGraphC);//QueryToJenaGraph.match(baseIso.inverse(), residualInsertGraphB, viewGraphC).collect(Collectors.toSet());
//            Iterable<BiMap<V, V>> isosBC = isoMatcher.match(transBaseIsoBC, residualInsertGraphB, viewGraphC);//QueryToJenaGraph.match(baseIso.inverse(), residualInsertGraphB, viewGraphC).collect(Collectors.toSet());
////            isosBC = Lists.newArrayList(isosBC);
////            System.out.println("Worked A!");
//            for(BiMap<V, V> isoBC : isosBC) {
//                isSubsumedC = true;
//                writer.println("Detected subsumption #" + ++i + " with iso: " + isoBC);
//                writer.incIndent();
//
//                // We found an is from B to C, where there was a prior iso from A to C
//                // This means, we need to update the transIso of C as if we were coming from B instead of A
//
//                // TODO FUCK! This isoGraph object may be a reason to keep the original graph and the iso in a combined graph object
//                //nodeB = nodeB == null ? createNode(residualInsertGraphB, isoAB) : nodeB;
//                G mappedResidualInsertGraphC = setOps.applyIso(residualInsertGraphB, isoBC);
//                Set<T> mappedResidualInsertGraphCTags = residualInsertGraphBTags;
//                G removalGraphC = setOps.intersect(mappedResidualInsertGraphC, viewGraphC);
//
//                Set<T> removalGraphCTags = Sets.intersection(mappedResidualInsertGraphCTags, viewGraphCTags);
//
//
//                BiMap<V, V> transIsoBAC = chain(baseIsoAB.inverse(), transIsoAC);
//
//                // The isoBC must be a subset of transIsoAC (because A subgraphOf B subgraphOf C)
//                // Get the mappings that are in common, so we can subtract them
//                BiMap<V, V> removalIsoBC = HashBiMap.create(Maps.difference(isoBC, transIsoBAC).entriesInCommon());
//
//
//                 // BiMap<V, V> deltaIsoABC = HashBiMap.create(Maps.difference(isoBC, transIsoAC).entriesInCommon());
//                //System.out.println("deltaIsoBC: " + deltaIsoBC);
//
//                //BiMap<V, V> removalTransIsoBC = HashBiMap.create(Maps.difference(isoBC, transIsoAC).entriesInCommon());
//
//
////                BiMap<V, V> newTransIsoC = mapDomainVia(nodeC.getTransIso(), isoBC);
//                //BiMap<V, V> newTransIsoC = mapDomainVia(nodeC.getTransIso(), isoBC);
//                //System.out.println("NewTransIsoC: " + newTransIsoC);
//
//
//
//                GraphIndexNode<K, G, V, T> newChildC = cloneWithRemoval(nodeC, isoBC, removalIsoBC, removalGraphC, removalGraphCTags, writer);
//                nodeB.appendChild(newChildC);//add(newChild, baseIso, writer);
//
//
//                writer.decIndent();
//            }
//
//            if(isSubsumedC) {
//                deleteNode(nodeC.getKey());
//            }
//
//
////            if(nodeB != null) {
////                //it.remove();
////
////                //nodeB.getKeys().add(key);
////
////                writer.println("A node was subsumed and therefore removed");
////                //wasAdded = true;
////                // not sure if this remove works
////            }
//            writer.decIndent();
//
//        }
//        writer.decIndent();
//
//    }
//
//    // If nothing was subsumed, add it to this node
//    //if(!wasAdded) {
//        writer.println("Attached graph of size " + setOps.size(residualInsertGraphB) + " to node " + nodeA);
//        nodeA.appendChild(nodeB);
//        //GraphIndexNode<K> target = createNode(residualInsertGraphB, baseIso);
//        //target.getKeys().add(key);
//        //nodeA.appendChild(target);
//    //}
//}


//
//Iterable<BiMap<Node, Node>> isoTmp = Lists.newArrayList(toIterable(QueryToJenaGraph.match(baseIso.inverse(), insertGraph, viewGraph)));
//
//GraphVar ga = new GraphVarImpl();
////insertGraph.find(Node.ANY, Node.ANY, Node.ANY).forEachRemaining(ga::add);
//GraphUtil.addInto(ga, insertGraph);
//ga.find(Node.ANY, Node.ANY, Var.alloc("ao")).forEachRemaining(x -> System.out.println(x));
//GraphVar gb = new GraphVarImpl();
//viewGraph.find(Node.ANY, Node.ANY, Node.ANY).forEachRemaining(gb::add);
////GraphUtil.addInto(gb, viewGraph);
//insertGraph = ga;
//viewGraph = new GraphIsoMapImpl(gb, HashBiMap.create());

//                    System.out.println("Remapped insert via " + iso);
//RDFDataMgr.write(System.out, insertGraphX, RDFFormat.NTRIPLES);
//System.out.println("---");

//Difference retain = new Difference(viewGraph, insertGraphX);

// The part which is duplicated between the insert graph and the view
// is subject to removal
//Intersection removalGraph = new Intersection(mappedInsertGraph, viewGraphC);

// Allocate root before child to give it a lower id for cosmetics
//nodeB = nodeB == null ? createNode(mappedInsertGraph) : nodeB;



//protected ProblemNeighborhoodAware<BiMap<V, V>, V> toProblem(InsertPosition<?, G, V> pos) {
//  BiMap<V, V> baseIso = pos.getIso();
//  G residualQueryGraph = pos.getResidualQueryGraph();
//
//  // TODO This looks wrong, why an empty graph here?!
//  G residualViewGraph = setOps.createNew(); // new
//                                              // GraphVarImpl();//pos.getNode().getValue();
//                                              // //new
//                                              // GraphIsoMapImpl(pos.getNode().getValue(),
//                                              // pos.getNode().getTransIso());
//                                              // //pos.getNode().getValue();
//
//  // QueryToJenaGraph::createNodeComparator,
//  // QueryToJenaGraph::createEdgeComparator);
//  ProblemNeighborhoodAware<BiMap<V, V>, V> result = isoMatcher.match(baseIso, residualViewGraph, residualQueryGraph);
//
//  return result;
//}

//protected Iterable<BiMap<V, V>> toProblem(InsertPosition<?, G, V, ?> pos) {
//  BiMap<V, V> baseIso = pos.getIso();
//  G residualQueryGraph = pos.getResidualQueryGraph();
//
//  // TODO This looks wrong, why an empty graph here?!
//  G residualViewGraph = setOps.createNew(); // new
//                                              // GraphVarImpl();//pos.getNode().getValue();
//                                              // //new
//                                              // GraphIsoMapImpl(pos.getNode().getValue(),
//                                              // pos.getNode().getTransIso());
//                                              // //pos.getNode().getValue();
//
//  // QueryToJenaGraph::createNodeComparator,
//  // QueryToJenaGraph::createEdgeComparator);
//  Iterable<BiMap<V, V>> result = isoMatcher.match(baseIso, residualViewGraph, residualQueryGraph);
//
//  return result;
//}
//
//protected Iterable<BiMap<V, V>> createProblem(Collection<? extends InsertPosition<?, G, V, ?>> poss) {
//  Iterable<BiMap<V, V>> result = () -> poss.stream()
//          //The following two lines are equivalent to .flatMap(pos -> Streams.stream(toProblem(pos)))
//          .map(this::toProblem)
//          .flatMap(Streams::stream)
//          .distinct()
//          .iterator();
//          //.collect(Collectors.toList());
//
//  return result;
//}
//


//
//public Map<K, ProblemNeighborhoodAware<BiMap<V, V>, V>> lookupStream2(G queryGraph, boolean exactMatch) {
//  Multimap<K, InsertPosition<K, G, V, T>> matches = lookup(queryGraph, exactMatch);
//
//  Map<K, ProblemNeighborhoodAware<BiMap<V, V>, V>> result =
//      matches.asMap().entrySet().stream()
//          .collect(Collectors.toMap(
//                  Entry::getKey,
//                  e -> createProblem(e.getValue())
//                  ));
//
//  return result;
////              Map<K, ProblemNeighborhoodAware<BiMap<Var, Var>, Var>> result = matches.asMap().entrySet().stream()
////                      .collect(Collectors.toMap(e -> e.getKey(), e -> SparqlViewMatcherQfpcIso.createCompound(e.getValue())));
////
////
////  BiMap<Node, Node> baseIso = pos.getIso();
////
////
////  System.out.println("RAW SOLUTIONS for " + pos.getNode().getKey());
////  rawProblem.generateSolutions().forEach(s -> {
////      System.out.println("  Raw Solution: " + s);
////  });
////
////  ProblemNeighborhoodAware<BiMap<Var, Var>, Var> result = new ProblemVarWrapper(rawProblem);
////
////
////  return result;
//}



//Map<Long, Iterable<BiMap<V, V>>> tmp =
//  matches.asMap().entrySet().stream()
//      .collect(Collectors.toMap(
//              Entry::getKey,
//              e -> createProblem(e.getValue())));


//Map<K, Iterable<BiMap<V, V>>> result = tmp.entrySet().stream()
//  .flatMap(e -> idToKeys.get(e.getKey()).stream().map(key -> new SimpleEntry<>(key, e.getValue())))
//  .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

// TODO Include all alternative keys
//Map<K, Iterable<BiMap<V, V>>> result = tmp.entrySet().stream()
//.flatMap(e -> idToPrefKey.get(e.getKey()).stream().map(key -> new SimpleEntry<>(key, e.getValue())))
//.map(e -> new SimpleEntry<>(idToPrefKey.get(e.getKey()), e.getValue()))
//.collect(Collectors.toMap(Entry::getKey, Entry::getValue));




//
///**
//* Clones a sub tree thereby removing the triples in the removal graph
//* TODO: How to update the remaining isomorphisms?
//*
//*
//*
//*
//* @param removalGraphC
//* @param writer
//* @return
//*/
//// Note: isoBC will be equivalent to nodeC.getTransIso() on recursion, but the first call will override it
////       - so this set of tags depends on the parent node
//GraphIndexNode<K, G, V, T> cloneWithRemoval(GraphIndexNode<K, G, V, T> nodeC, BiMap<V, V> baseIso, BiMap<V, V> removalTransIsoBC, G removalGraphC, Set<T> removalGraphCTags, IndentedWriter writer) { //BiMap<Node, Node> isoBC, Graph residualInsertGraphB,
//  G graphC = nodeC.getValue();
//
//  G residualGraphC = setOps.difference(graphC, removalGraphC);
//  Set<T> residualGraphCTags = Sets.difference(nodeC.getGraphTags(), removalGraphCTags);
//
//  if(logger.isDebugEnabled()) {
//      logger.debug("At node " + nodeC.getId() + ": Cloned graph size reduced from  " + setOps.size(graphC) + " -> " + setOps.size(residualGraphC));
//  }
//
//  BiMap<V, V> isoNewBC = HashBiMap.create(
//          Maps.difference(nodeC.getTransIso(), removalTransIsoBC).entriesOnlyOnLeft());
//
//  //BiMap<V, V> isoNewBC = HashBiMap.create(nodeC.getTransIso());
//  //isoNewBC.entrySet().removeAll(isoBC.entrySet());
////          BiMap<V, V> deltaIso = HashBiMap.create(
////                  Maps.inte(iso, transBaseIso).entriesOnlyOnLeft());
//
//  GraphIndexNode<K, G, V, T> newNodeC = createNode(residualGraphC, residualGraphCTags, isoNewBC);
//
//
////  GraphIndexNode<K, G, V, T> newNodeC = createNode(residualGraphC, residualGraphCTags, isoBC);
//  //newNodeC.getKeys().addAll(nodeC.getKeys());
//
//
//  // Then for each child: map the removal graph according to the child's iso
//  for(GraphIndexNode<K, G, V, T> nodeD : nodeC.getChildren()) {
//
//      BiMap<V, V> isoCD = nodeD.getTransIso();
//
//      BiMap<V, V> newBaseIso = mapDomainVia(baseIso, isoCD);
//
////      GraphIsoMap removalGraphD = new GraphIsoMapImpl(removalGraphC, isoCD);
//      G removalGraphD = setOps.applyIso(removalGraphC, newBaseIso); //isoCD);
//
//      BiMap<V, V> removalTransIsoCD = mapDomainVia(removalTransIsoBC, isoCD);
//
//      // NOTE Graph tags are unaffected by isomorphism
//      Set<T> removalGraphDTags = removalGraphCTags;
//
//      GraphIndexNode<K, G, V, T> cloneChild = cloneWithRemoval(nodeD, newBaseIso, removalTransIsoCD, removalGraphD, removalGraphDTags, writer);
//      //deleteNode(child.getKey());
//      newNodeC.appendChild(cloneChild);
//  }
//
//  long[] nodeIds = nodeC.getChildren().stream().mapToLong(GraphIndexNode::getId).toArray();
//  for(Long nodeId : nodeIds) {
//      deleteNode(nodeId);
//  }
//
//
//
//  return newNodeC;
//}



// Iterate all children whose graphTags are a super set of the given one
// Thereby recursively updating the lookup tag set with the graphTags covered by a node
//Stream<GraphIndexNode<K, G, V, T>> candChildren = lookupProvidedChildrenByTags(
//        directCandChildren.stream(),
//        residualInsertGraphBTags,
//        (node, tags) -> Sets.difference(tags, node.getGraphTags()),
//        (node, tags) -> node.childIndex.getAllSupersetsOf(tags, false)
//                .keySet().stream().map(nodeId -> nodeA.idToChild.get(nodeId))
//        );


//Collection<GraphIndexNode<K, G, V, T>> tmp = candChildren.collect(Collectors.toList());
//
//candChildren = tmp.stream();
//
//// For every key in the sub-tree, collect one representative
//// residual graph.
//Map<K, G> keyToGraph = new HashMap<>();
//Multimap<K, K> altKeys = HashMultimap.create();
//candChildren.forEach(node -> {
//    // Pick one key as the representative key and use the others as alt keys
//    Collection<K> keys = new HashSet<>(idToKeys.get(node.getId()));
//    idToKeys.removeAll(node.getId());
//
//    if(!keys.isEmpty()) {
//        K repKey = keys.iterator().next();
//        for(K altKey : keys) {
//            if(!Objects.equals(repKey, altKey)) {
//                altKeys.put(repKey, altKey);
//            }
//        }
//
//        // For the repKey, build the graph
//        // The baseGraph is this node's parent keyGraph
//        Collection<K> parentKeys = keys;///idToKeys.get(node.getId());
//        K parentRepKey = Iterables.getFirst(parentKeys, null);
//        G baseGraph = keyToGraph.get(parentRepKey);
//        G keyGraph = baseGraph == null
//                ? node.getValue()
//                : setOps.union(setOps.applyIso(baseGraph, node.transIso), node.graph)
//                ;
//
//        keyToGraph.put(repKey, keyGraph);
//    }
//});
