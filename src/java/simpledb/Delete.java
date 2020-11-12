package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId _tid;
    private DbIterator _child;
    private TupleDesc _td;
    private boolean _is_called;
    private int _total_deleted;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this._child = child;
        this._tid = t;
        this._td = new TupleDesc(new Type[] {Type.INT_TYPE});
        this._is_called = false;
        this._total_deleted = 0;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return _td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        _child.open();
    }

    public void close() {
        // some code goes here
        _child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        _child.rewind();
        _is_called = false;
        _total_deleted = 0;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (_is_called)
            return null;
        else {
            while (_child.hasNext()) {
                Tuple t = _child.next();
                try {
                    Database.getBufferPool().deleteTuple(_tid, t);
                    _total_deleted++;
                } catch (IOException e) {
                    Debug.log("[ERR] IOException when inserting Tuple(%s)!!!", t.toString());
                }
            }
            _is_called = true;
            Tuple result = new Tuple(_td);
            result.setField(0, new IntField(_total_deleted));
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
