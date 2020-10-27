package simpledb;

import java.io.IOException;
import java.util.zip.Inflater;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     * The transaction running the insert.
     * @param child
     * The child operator from which to read tuples to be inserted.
     * @param tableId
     * The table in which to insert tuples.
     * @throws DbException
     * if TupleDesc of child differs from table into which we are to
     * insert.
     */

    OpIterator child;
    int tableId;
    TransactionId transactionId;
    TupleDesc tupleDesc;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.transactionId = t;
        this.child = child;
        this.tableId = tableId;
        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
        TupleDesc childTupleDesc = child.getTupleDesc();
        TupleDesc tableDesc = Database.getCatalog().getTupleDesc(tableId);
        if (!tableDesc.equals(childTupleDesc)) {
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
        }
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();

    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
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
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(transactionId, tableId, child.next());
                count ++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (count != 0) {
            Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            tuple.setField(0, new IntField(count));
            return tuple;
        }
        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        child = children[0];
    }
}
