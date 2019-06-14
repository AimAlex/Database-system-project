package simpledb;

import java.io.IOException;

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
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */

    private TransactionId t;
    private DbIterator child;
    private int tableId;
    private TupleDesc td;
    private boolean fetch;

    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        this.t = t;
        this.child = child;
        this.tableId = tableId;
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
        fetch = true;
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException, InterruptedException {

        child.open();
        super.open();
        fetch = false;
        // some code goes here
    }

    public void close() {
        super.close();
        child.close();
        fetch = true;

        // some code goes here
    }

    public void rewind() throws DbException, TransactionAbortedException, InterruptedException {
        child.rewind();
        fetch = false;
        // some code goes here
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
    protected Tuple fetchNext() throws TransactionAbortedException, DbException, InterruptedException {
        // some code goes here
        if(fetch){
            return null;
        }
        fetch = true;
        int number = 0;
        while(child.hasNext()){
            Tuple tmp = child.next();
            Database.getBufferPool().insertTuple(t, tableId, tmp);
            ++number;
        }
        Tuple t = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        t.setField(0, new IntField(number));
        return t;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return null;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    }
}
