package simpledb;

/**
 * A class to represent a fixed-width histogram over a single String-based
 * field.
 */
public class SuffixTreeStringHistogram {

    private SuffixTree trie;
    private int _n_tuples;

    /**
     * Create a new StringHistogram with a specified number of buckets.
     * <p>
     * Our implementation is written in terms of an IntHistogram by converting
     * each String to an integer.
     *
     */
    public SuffixTreeStringHistogram() {
        trie = null;
        _n_tuples = 0;
    }

    /** Add a new value to suffix trie */
    public void addValue(String s) {
        if (trie == null) {
            trie = new SuffixTree(s);
        } else {
            trie.insertString(s);
        }
        this._n_tuples += 1;
    }

    /**
     * Estimate the selectivity (as a double between 0 and 1) of the specified
     * predicate over the specified string
     *
     * @param op
     *            The operation being applied
     * @param s
     *            The string to apply op to
     */
    public double estimateSelectivity(Predicate.Op op, String s) {
        if (op == Predicate.Op.LIKE) {
            return (trie.searchText(s) / this._n_tuples);
        }
        return 1.0;
    }

    /**
     * @return the average selectivity of this histogram.
     *
     *         This is not an indispensable method to implement the basic join
     *         optimization. It may be needed if you want to implement a more
     *         efficient optimization
     * */
    public double avgSelectivity() {
        return 1.0;//hist.avgSelectivity();
    }
}
