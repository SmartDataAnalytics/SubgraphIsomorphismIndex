package org.aksw.combinatorics.solvers.collections;

import java.util.Map.Entry;

import org.aksw.combinatorics.solvers.GenericProblem;

/***
 * A helper class used by ProblemContainer.
 * The pick method of a that class must return two result values:
 * <ol>
 *   <li>the picked Problem</li>
 *   <li>the remaining workload</li>
 * </ol>
 * This is what this class represents.
 *
 * Note, that this class inherits from Entry as it is essentially a Tuple2 and
 * can thus be used as such should that ever be needed
 *
 * @author raven
 *
 * @param <S> The solution type
 */
public class ProblemContainerPick<S>
    implements Entry<GenericProblem<S, ?>, ProblemContainer<S>>
{
    protected GenericProblem<S, ?> picked;
    protected ProblemContainer<S> remaining;

    public ProblemContainerPick(GenericProblem<S, ?> picked,
            ProblemContainer<S> remaining) {
        super();
        this.picked = picked;
        this.remaining = remaining;
    }
    public GenericProblem<S, ?> getPicked() {
        return picked;
    }
    public ProblemContainer<S> getRemaining() {
        return remaining;
    }

    @Override
    public GenericProblem<S, ?> getKey() {
        return picked;
    }
    @Override
    public ProblemContainer<S> getValue() {
        return remaining;
    }
    @Override
    public ProblemContainer<S> setValue(
            ProblemContainer<S> value) {
        throw new UnsupportedOperationException();
    }
}

