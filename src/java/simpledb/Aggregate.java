package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private DbIterator _child;
    private int _afield;
    private int _gfield;
    private Type _gbfieldtype;
    private Aggregator.Op _aggr_op;
    private Aggregator _aggregator;
    private DbIterator _aggr_iterator = null;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	    // some code goes here
        this._child = child;
        this._afield = afield;
        this._gfield = gfield;
        this._aggr_op = aop;

        // aggregator setup
        _gbfieldtype = null;
        if (_gfield != Aggregator.NO_GROUPING)
            _gbfieldtype = child.getTupleDesc().getFieldType(_gfield);

        if (_child.getTupleDesc().getFieldType(_afield) == Type.INT_TYPE) {
            Debug.log("Initialized IntegerAggregator!");
            this._aggregator = new IntegerAggregator(_gfield, _gbfieldtype, _afield, _aggr_op);
        } else {
            Debug.log("Initialized StringAggregator!");
            this._aggregator = new StringAggregator(_gfield, _gbfieldtype, _afield, _aggr_op);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	    // some code goes here
	    return _gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	    // some code goes here
	    return _child.getTupleDesc().getFieldName(_gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	    // some code goes here
	    return _afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return _child.getTupleDesc().getFieldName(_afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	    // some code goes here
        return _aggr_op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	    // some code goes here
        _child.open();
        super.open();
        if (_aggr_iterator == null) {
            while (_child.hasNext()) {
                _aggregator.mergeTupleIntoGroup(_child.next());
            }
            _aggr_iterator = _aggregator.iterator();
            _aggr_iterator.open();
        }
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	    // some code goes here

        while(_aggr_iterator.hasNext())
            return _aggr_iterator.next();
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	    // some code goes here
        _aggr_iterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	    // some code goes here
        TupleDesc td = null;
        String aggr_col_name = String.format("%s (%s)", nameOfAggregatorOp(_aggr_op), aggregateFieldName());
        if (_gfield == Aggregator.NO_GROUPING) {
            // (aggregateVal)
            td = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[] {aggr_col_name});
        } else {
            // (groupVal, aggregateVal)
            td = new TupleDesc(new Type[] {_gbfieldtype, Type.INT_TYPE}, new String[] {groupFieldName(), aggr_col_name});
        }
	    return td;
    }

    public void close() {
	    // some code goes here
        super.close();
        _child.close();
        _aggr_iterator.close();

    }

    @Override
    public DbIterator[] getChildren() {
	    // some code goes here
	    return new DbIterator[] { _child };
    }

    @Override
    public void setChildren(DbIterator[] children) {
	    // some code goes here
        if (_child != children[0])
            _child = children[0];
    }
    
}
