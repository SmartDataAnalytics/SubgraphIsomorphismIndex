package org.aksw.combinatorics.solvers.collections;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.aksw.combinatorics.solvers.GenericProblem;

import com.google.common.collect.Lists;

class Aggregate<S> {
    protected BinaryOperator<S> solutionCombiner;
    protected Predicate<S> isUnsatisfiable;
}

/**
 * A more sophisticated problem solver than {@link ProblemSolver} that
 * associates problems and their solutions with keys.
 *
 * When a overall solution is obtained, the contributions for each key are tracked.
 *
 *
 * @author raven
 *
 * @param <S>
 */
public class ProblemSolver2<S> {

    public static <L, V> Stream<Solution<V, L>> solve(
            L baseSolution,
            //Multimap<K, GenericProblem<S, ?>> keyToProblems,
            List<? extends Collection<? extends GenericProblem<L, ?>>> keyToProblem,
            BinaryOperator<L> contributionAggregator,
            Function<? super L, ? extends V> toResultSolutionType,
            BinaryOperator<V> solutionAggregator,
            Function<? super V, ? extends L> resultToRefinementType
            ) {

        List<List<L>> table = new ArrayList<>();
        //for(Entry<Collection<? extends GenericProblem<S, ?>>> e : keyToProblems.asMap().entrySet()) {
        IntStream.range(0, keyToProblem.size()).forEach(i -> {
            Collection<? extends GenericProblem<L, ?>> problems = keyToProblem.get(i);

            Stream<L> contribs;
            if(problems.size() == 1) {
                contribs = problems.iterator().next().generateSolutions();
            } else {
                contribs =
                    ProblemSolver.solve(problems, baseSolution, contributionAggregator);
            }

            List<L> item = contribs.collect(Collectors.toList());

            table.add(item);
        });


        List<List<L>> cart = Lists.cartesianProduct(table);


        // Try to create an overall solution
        // if that works, return an object with the overall solution and the contributions per key
        Stream<Solution<V, L>> result = cart.stream()
            .map(contribs -> {

                V overall = contribs.stream()
                    .map(toResultSolutionType)
                    .map(x -> (V)x)
                    .map(Optional::of) // wrap as optionals
                    .reduce((a, b) ->
                        a.isPresent() && b.isPresent()
                            ? Optional.ofNullable(solutionAggregator.apply((V)a.get(), (V)b.get()))
                            : Optional.empty()
                    )
                    .map(x -> x.orElse(null))
                    .orElse(null);

                Solution<V, L> r = overall == null ? null : new Solution<>(overall, contribs);

                return r;
            })
            .filter(x -> x != null);

        return result;
    }


}
