package org.aksw.commons.graph.index.core;

import java.util.function.Supplier;

public class SetOpsImpl<S> {
    protected Supplier<S> create;

    public SetOpsImpl(Supplier<S> create) {
        this.create = create;
    }
    
    

}
