package org.aksw.combinatorics.solvers;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProblemUnion<S, T>
    implements ProblemNeighborhoodAware<S, T>
{
    protected Collection<? extends ProblemNeighborhoodAware<S, T>> problems;

    public ProblemUnion(Collection<? extends ProblemNeighborhoodAware<S, T>> problems) {
        super();
        this.problems = problems;
    }

    @Override
    public Stream<S> generateSolutions() {
        Stream<S> result = problems.stream().flatMap(p -> p.generateSolutions());
        return result;
    }

    @Override
    public Collection<? extends ProblemNeighborhoodAware<S, T>> refine(S partialSolution) {
        Collection<? extends ProblemNeighborhoodAware<S, T>> result =
                problems.stream()
                .flatMap(p -> p.refine(partialSolution).stream())
                .collect(Collectors.toList());
        return result;
    }

    @Override
    public boolean isEmpty() {
        boolean result = problems.stream().allMatch(p -> p.isEmpty());
        return result;
    }

    @Override
    public long getEstimatedCost() {
        long result = problems.stream().mapToLong(CostAware::getEstimatedCost).reduce(0, Long::sum);
        return result;
    }

    @Override
    public Collection<T> getSourceNeighbourhood() {
        // Return the union of all neightbourhoods
        Set<T> result = problems.stream()
                .flatMap(p -> p.getSourceNeighbourhood().stream())
                .collect(Collectors.toSet());
        return result;
    }

}
