package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId _tid;
    private DbIterator _child;
    private int _tableId;
    private TupleDesc _td;
    private boolean _is_called;
    private int _total_inserted;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this._child = child;
        this._tableId = tableId;
        this._tid = t;
        this._td = new TupleDesc(new Type[] {Type.INT_TYPE});
        this._is_called = false;
        this._total_inserted = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return _td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        _child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        // TODO: check  this
        _child.rewind();
        _is_called = false;
        _total_inserted = 0;
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (_is_called)
            return null;
        else {
            while (_child.hasNext()) {
                Tuple t = _child.next();
                try {
                    Database.getBufferPool().insertTuple(_tid, _tableId, t);
                    _total_inserted++;
                } catch (IOException e) {
                    Debug.log("[ERR] IOException when inserting Tuple(%s)!!!", t.toString());
                }
            }
            _is_called = true;
            Tuple result = new Tuple(_td);
            result.setField(0, new IntField(_total_inserted));
            return result;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[] { this._child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (children[0] != _child)
            _child = children[0];
    }
}
