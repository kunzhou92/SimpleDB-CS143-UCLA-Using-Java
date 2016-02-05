package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {
	
	private Predicate _p;
	private DbIterator _child;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
    	_p = p;
    	_child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return _p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return _child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
    	super.open();
    	_child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
    	_child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	_child.rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
    	Tuple temp;
    	while(_child.hasNext())
    	{
    		temp = _child.next();
    		if(_p.filter(temp))
    			return temp;
    	}
    	return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	DbIterator[] result = {_child};
        return result;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	_child = children[0];
    }

}
