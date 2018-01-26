package org.aksw.combinatorics.solvers.collections;

import java.util.List;
import java.util.Map;

public class Solution<S, C> {
	protected S solution;
	protected List<C> contributions;
	
	public Solution(S solution, List<C> contributions) {
		super();
		this.contributions = contributions;
		this.solution = solution;
	}

	public List<C> getContributions() {
		return contributions;
	}

	public S getSolution() {
		return solution;
	}
}
