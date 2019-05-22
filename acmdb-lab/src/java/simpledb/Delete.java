package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */

    private TransactionId t;
    private DbIterator child;
    private TupleDesc td;
    private boolean fetch;

    public Delete(TransactionId t, DbIterator child) {
        this.t = t;
        this.child = child;
        td = new TupleDesc(new Type[]{Type.INT_TYPE});
        fetch = true;
        // some code goes here
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
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

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        fetch = false;
        // some code goes here
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
        if(fetch){
            return null;
        }
        fetch = true;
        int number = 0;
        while(child.hasNext()){
            Tuple tmp = child.next();
            try{
                Database.getBufferPool().deleteTuple(t, tmp);
            } catch (IOException e) {
                e.printStackTrace();
            }
            ++number;
        }
        Tuple count = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
        count.setField(0, new IntField(number));
        return count;

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
