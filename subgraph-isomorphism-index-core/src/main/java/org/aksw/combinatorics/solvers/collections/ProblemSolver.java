package org.aksw.combinatorics.solvers.collections;

import java.util.Collection;
import java.util.Collections;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

import org.aksw.combinatorics.solvers.GenericProblem;

/**
 * Default implementation of the core algorithm for solving problems.
 * TODO Give it some fancy name
 *
 * @author Claus Stadler
 *
 * @param <S> The solution type
 */
public class ProblemSolver<S> {
    protected ProblemContainer<S> problemContainer;
    protected S baseSolution;
    protected BinaryOperator<S> solutionCombiner;

    public ProblemSolver(ProblemContainer<S> problemContainer, S baseSolution, BinaryOperator<S> solutionCombiner) {
        super();
        this.problemContainer = problemContainer;
        this.baseSolution = baseSolution;
        this.solutionCombiner = solutionCombiner;
    }

    public Stream<S> streamSolutions() {

        Stream<S> result;
        if(problemContainer.isEmpty()) {
            result = Stream.empty();
        } else {

            ProblemContainerPick<S> pick = problemContainer.pick();

            GenericProblem<S, ?> picked = pick.getPicked();
            ProblemContainer<S> remaining = pick.getRemaining();

            result = picked
                .generateSolutions()
                .flatMap(solutionContribution -> {
                    S partialSolution = solutionCombiner.apply(baseSolution, solutionContribution);

                    Stream<S> r;
                    // If the partial solution is null, then indicate the
                    // absence of a solution by returning a stream that yields
                    // null as a 'solution'
                    if (partialSolution == null) {
                        r = Collections.<S> singleton(null).stream();
                    } else {
                        // This step is optional: it refines problems
                        // based on the current partial solution
                        // Depending on your setting, this can give a
                        // performance boost or penalty
                        //ProblemContainerImpl<S> openProblems = remaining;
                        ProblemContainer<S> openProblems = remaining.refine(partialSolution);

                        if (openProblems.isEmpty()) {
                            //r = Collections.<S> emptySet().stream();
                            r = Collections.singleton(partialSolution).stream();
                        } else {
                            ProblemSolver<S> nextState = new ProblemSolver<S>(openProblems, baseSolution, solutionCombiner);
                            r = nextState.streamSolutions();
                        }
                    }
                    return r;
                });
        }
        return result;
    }

    public static <S> Stream<S> solve(Collection<? extends GenericProblem<S, ?>> problems, S baseSolution, BinaryOperator<S> solutionCombiner) {
        ProblemContainer<S> problemContainer = ProblemContainerImpl.create(problems);
    	ProblemSolver<S> problemSolver = new ProblemSolver<S>(problemContainer, baseSolution, solutionCombiner);
        Stream<S> result = problemSolver.streamSolutions();

        return result;
    }

    public static <S> Stream<S> solve(ProblemContainer<S> problemContainer, S baseSolution, BinaryOperator<S> solutionCombiner) {
        ProblemSolver<S> problemSolver = new ProblemSolver<S>(problemContainer, baseSolution, solutionCombiner);
        Stream<S> result = problemSolver.streamSolutions();

        return result;
    }
}
