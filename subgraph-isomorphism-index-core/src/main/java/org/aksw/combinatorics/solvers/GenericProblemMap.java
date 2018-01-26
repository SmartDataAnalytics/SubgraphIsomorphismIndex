package org.aksw.combinatorics.solvers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

class GenericProblemMap<T, S>
	implements GenericProblem<T, GenericProblemMap<T, S>>
{
	protected GenericProblem<S, ?> delegate;
	protected Function<? super S, ? extends T> fn;
	protected Function<? super T, ? extends S> getRefinementArg;

	public GenericProblemMap(GenericProblem<S, ?> delegate, Function<? super S, ? extends T> fn,
			Function<? super T, ? extends S> getRefinementArg) {
		super();
		this.delegate = delegate;
		this.fn = fn;
		this.getRefinementArg = getRefinementArg;
	}

	@Override
	public long getEstimatedCost() {
		return delegate.getEstimatedCost();
	}

	@Override
	public Stream<T> generateSolutions() {
		return delegate.generateSolutions().map(fn);
	}

	@Override
	public Collection<GenericProblemMap<T, S>> refine(T partialSolution) {
		S arg = getRefinementArg.apply(partialSolution);

		List<GenericProblemMap<T, S>> result = new ArrayList<>();
		
		for(GenericProblem<S, ?> original : delegate.refine(arg)) {
			GenericProblemMap<T, S> mapped = original.map(fn, getRefinementArg);
			result.add(mapped);
		}
		
//		List<GenericProblemMap<S, T>> result = delegate.refine(arg)
//			.stream()
//			.map(p -> p.map(fn, getRefinementArg))
//			.collect(Collectors.toList());
		
		return result;
	}

	@Override
	public boolean isEmpty() {
		return delegate.isEmpty();
	}
}