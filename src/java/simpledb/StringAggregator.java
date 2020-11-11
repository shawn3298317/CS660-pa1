package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static java.lang.Integer.max;
import static java.lang.Integer.min;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _aggr_op;
    private HashMap<Field, Integer[]> _aggregated_groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param aggr_op aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if aggr_op != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op aggr_op) {
        // some code goes here
        this._gbfield = gbfield;
        this._gbfieldtype = gbfieldtype;
        this._afield = afield;
        if (aggr_op == Op.COUNT) {
            this._aggr_op = aggr_op;
        } else {
            throw new IllegalArgumentException("Aggregation Op only supports `COUNT`");
        }
        this._aggregated_groups = new HashMap<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        Field groupVal = null;
        // IntField aggrVal = (IntField) tup.getField(_afield);
        if (_gbfield != Aggregator.NO_GROUPING) {
            groupVal = tup.getField(_gbfield);
        }

        if (_aggregated_groups.containsKey(groupVal)) {
            Integer[] cur_result = _aggregated_groups.get(groupVal);
            cur_result[0] = cur_result[0] + 1;

        } else {
            // Integer v = aggrVal.getValue();
            Integer[] toAdd = new Integer[1];
            toAdd[0] = 1;
            _aggregated_groups.put(groupVal, toAdd);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here

        // Determine TupleDesc for the aggr_result
        TupleDesc td = null;
        if (_gbfield == Aggregator.NO_GROUPING) {
            // (aggregateVal)
            td = new TupleDesc(new Type[] {Type.INT_TYPE});
        } else {
            // (groupVal, aggregateVal)
            td = new TupleDesc(new Type[] {_gbfieldtype, Type.INT_TYPE});
        }

        ArrayList<Tuple> tuplist = new ArrayList<>();
        for (Map.Entry<Field, Integer[]> entry: _aggregated_groups.entrySet()) {
            Tuple tup = new Tuple(td);
            if (_gbfield == Aggregator.NO_GROUPING) {
                tup.setField(0, new IntField(entry.getValue()[0]));
            } else {
                tup.setField(0, entry.getKey());
                tup.setField(1, new IntField(entry.getValue()[0]));
            }
            tuplist.add(tup);
        }

        return new TupleIterator(td, tuplist);
        // throw new UnsupportedOperationException("please implement me for lab3");
    }

}
