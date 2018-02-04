package org.aksw.commons.jena.jgrapht;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Statement;
import org.jgrapht.EdgeFactory;


public class EdgeFactoryJenaModel
    implements EdgeFactory<RDFNode, Statement>
{
    protected Model model;
    protected Property property;

    public EdgeFactoryJenaModel(Model model, Property property) {
        super();
        this.model = model;
        this.property = property;
    }

    @Override
    public Statement createEdge(RDFNode sourceVertex, RDFNode targetVertex) {
//        if(property == null) {
//        	throw new UnsupportedOperationException("Cannot create edge if property is null");
//        }

        Statement result = model.createStatement(sourceVertex.asResource(), property, targetVertex);
        return result;
    }
}