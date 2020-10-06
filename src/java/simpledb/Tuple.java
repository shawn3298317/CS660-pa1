package simpledb;

// import sun.security.ssl.Record;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static simpledb.Type.STRING_LEN;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;

    private TupleDesc _tupleDesc;
    private RecordId _recordId;
    private ArrayList<Field> _tupleFields = new ArrayList<>();

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        // Check validity for TupleDesc
        _tupleDesc = td;

        if (_tupleDesc.numFields() == 0)
            return;

        for (int i = 0; i < _tupleDesc.numFields(); i++) {
            Type t = _tupleDesc.getFieldType(i);
            Field toAdd;
            if (t == Type.INT_TYPE) {
                toAdd = new IntField(-1);
            } else {
                toAdd = new StringField("NULL", Type.STRING_LEN);
            }
            _tupleFields.add(toAdd);
        }
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return _tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return _recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        _recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i >= 0 && i < _tupleFields.size())
            _tupleFields.set(i, f);
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        if (i >= 0 && i < _tupleFields.size())
            return _tupleFields.get(i);
        else
            return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String content = "";
        for (int i = 0; i < _tupleFields.size(); i++) {
            Field f = _tupleFields.get(i);
            content += f.toString();
            if ( i != (_tupleFields.size() - 1))
                content += "\t";
        }
        return content;
        // throw new UnsupportedOperationException("Implement this");
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return _tupleFields.iterator();
    }

    /**
     * reset the TupleDesc of thi tuple
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        _tupleDesc = td;
        // TODO: do we need to check the new td with current fields?
    }
}
