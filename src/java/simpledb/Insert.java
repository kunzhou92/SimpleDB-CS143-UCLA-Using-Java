package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    
    private TransactionId _t;
    private DbIterator _child;
    private int _tableid;
    private boolean insert;
    private TupleDesc _TupleDesc; 

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // some code goes here
    	_t = t;
    	_child = child;
    	_tableid = tableid;
    	insert = false;
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
    	insert = false;
    }

    public void close() {
        // some code goes here
    	_child.close();
    	super.close();
    	insert = false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	_child.rewind();
    	insert = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
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
    	
    	if(insert)
    		return null;
    	int num = 0;
    	Tuple _count = new Tuple(_TupleDesc);
    	while(_child.hasNext())
    	{
    		try
    		{
    			Database.getBufferPool().insertTuple(_t, _tableid, _child.next());
    		} catch(IOException e)
    		{ 
    			throw new DbException("IOException");
    		}
    		num++;
    	}
    	_count.setField(0, new IntField(num));
    	insert = true;
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
