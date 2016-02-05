package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId _t;
    private DbIterator _child;
    private boolean delete;
    private TupleDesc _TupleDesc; 

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
    	_t = t;
    	_child = child;
    	delete = false;
    	_TupleDesc = new TupleDesc(new Type[] {Type.INT_TYPE}, new String[]{"count"});
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return _TupleDesc;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	_child.open();
    	super.open();
    	delete = false;
    }

    public void close() {
        // some code goes here
    	_child.close();
    	super.close();
    	delete = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	_child.rewind();
    	delete = false;
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
    	if(delete)
    		return null;
    	int num = 0;
    	Tuple _count = new Tuple(_TupleDesc);
    	while(_child.hasNext())
    	{
    		try
    		{
    			Database.getBufferPool().deleteTuple(_t, _child.next());
    		} catch(IOException e)
    		{ 
    			throw new DbException("IOException");
    		}
    		num++;
    	}
    	_count.setField(0, new IntField(num));
    	delete = true;
        return _count;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[] {_child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	_child = children[0];
    }

}
