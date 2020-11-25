package simpledb;

import static com.sun.javafx.util.Utils.sum;
import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.Math.abs;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int _n_buckets;
    private int _min;
    private int _max;
    private int _n_tuples;
    private double[] _hist_freq;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this._n_buckets = buckets;
        this._min = min;
        this._max = max;
        this._n_tuples = 0;
        this._hist_freq = new double[_n_buckets];
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucket = getBucket(v);
        // Debug.log("v: %d; bucket: %d; [%d, %d]/%d", v, bucket, _min, _max, _n_buckets);
        _hist_freq[bucket] += 1.0;
        _n_tuples += 1;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        v = max(min(_max, v), _min);
        int bucket = getBucket(v);

        if (op == Predicate.Op.EQUALS) { return _hist_freq[bucket]/_n_tuples; }
        else if (op == Predicate.Op.NOT_EQUALS) { return (1.0 - _hist_freq[bucket]/_n_tuples); }
        else if (op == Predicate.Op.GREATER_THAN || op == Predicate.Op.GREATER_THAN_OR_EQ) {

            double w_b = ((double)_max - _min) / _n_buckets;
            double b_right = _min + (bucket+1) * w_b;
            double acum_selectivity = (b_right - v) / w_b * _hist_freq[bucket];
//            Debug.log("bucket: %d, w_b: %f, b_right: %f; v = %d; selectivity: %f", bucket, w_b, b_right, v, acum_selectivity);
            for (int i = bucket+1; i < _n_buckets; i++) {
                acum_selectivity += (_hist_freq[i]);
            }
            acum_selectivity /= _n_tuples;
            return acum_selectivity;

        } else if (op == Predicate.Op.LESS_THAN || op == Predicate.Op.LESS_THAN_OR_EQ) {

            double w_b = ((double)_max - _min) / _n_buckets;
            double b_left = _min + bucket * w_b;
            double acum_selectivity = (v - b_left) / w_b * _hist_freq[bucket];
            Debug.log("bucket: %d, w_b: %f, b_right: %f; v = %d; (%f); selectivity: %f", bucket, w_b, b_left, v, _hist_freq[bucket], acum_selectivity);
            for (int i = bucket-1; i >= 0; i--) {
                acum_selectivity += (_hist_freq[i]);
                Debug.log("[%d] + %f => %f", i, _hist_freq[i], acum_selectivity);
            }
            acum_selectivity /= _n_tuples;
            Debug.log("Final select: %f", acum_selectivity);
            return acum_selectivity;
        }

        return 1.0;

    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here

        // selectivity = avg(sum(selectivity for each bucket))
        return sum(_hist_freq)/_n_buckets/_n_tuples;
        // return 1.0;
    }

    private int getBucket(int v) {
        int bucket = (int) (((double) v - _min) / (_max - _min) * _n_buckets);
        return min(_n_buckets-1, bucket);
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return String.format("Hist: %s; Avg_Selectivity: %f", _hist_freq.toString(), avgSelectivity());
    }
}
