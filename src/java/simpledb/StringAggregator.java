package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private ArrayList<Tuple> GroupAggre;
    private TupleDesc _TupleDesc;
    private boolean FirstTime = true;
    


    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	_gbfield = gbfield;
    	_gbfieldtype = gbfieldtype;
    	_afield = afield;
    	if(what != Op.COUNT)
    		throw new IllegalArgumentException();
    	_what = what;
    	GroupAggre = new ArrayList<Tuple>();
    }
    		
  
    
    
    /**
     * Find the specified group in GroupValue 
     * 
     * @param group
     *            the specified group
     * @return If the group is found, return the index, otherwise return -1
     *                            
     */
    private int findGroup(Field group)
    {
    	for(int i=0; i<GroupAggre.size(); i++)
    	{
    		if(GroupAggre.get(i).getField(0).equals(group))
    			return i;
    	}
    	return -1;
    	
    }
    

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
    	if(FirstTime)
    	{
    		if(_gbfield == NO_GROUPING)
    		{
    			Type[] typeAr = {Type.INT_TYPE};
    			String[] fieldAr = {tup.getTupleDesc().getFieldName(_afield)};
    			_TupleDesc = new TupleDesc(typeAr, fieldAr);
    		}
    		else
    		{
    			Type[] typeAr = {_gbfieldtype, Type.INT_TYPE};
    			String[] fieldAr = {tup.getTupleDesc().getFieldName(_gbfield), 
    					tup.getTupleDesc().getFieldName(_afield)};
    			_TupleDesc = new TupleDesc(typeAr, fieldAr);
    		}
    		FirstTime = false;
    	}
    	if(_gbfield == NO_GROUPING)
    	{
    		if(GroupAggre.size() == 0)
			{
    			Tuple tempTuple = new Tuple(_TupleDesc);
				tempTuple.setField(0, new IntField(1) );
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(0);
				IntField tempIntField = (IntField)(tempTuple.getField(0));
				tempTuple.setField(0, new IntField(tempIntField.getValue() + 1));
			}
    	}
    	else
    	{
    		int index = findGroup(tup.getField(_gbfield));
    		if( index== -1)
    		{
    			Tuple tempTuple = new Tuple(_TupleDesc);
    			tempTuple.setField(0, tup.getField(_gbfield));
    			tempTuple.setField(1, new IntField(1));
    			GroupAggre.add(tempTuple);
    		}
    		else
    		{
    			Tuple tempTuple = GroupAggre.get(index);
    			IntField tempIntField = (IntField)(tempTuple.getField(1)); 			
    			tempTuple.setField(1, new IntField(tempIntField.getValue() + 1));
    		}
    	}
    }
    
    private class StringAggreIterator implements DbIterator
    {
    	private static final long serialVersionUID = 1L;
    	
    	private Iterator<Tuple> GroupAggreIterator;
    	
    	public void open() throws DbException, TransactionAbortedException
    	{
    		GroupAggreIterator = GroupAggre.iterator();
    	}
    	public boolean hasNext() throws DbException, TransactionAbortedException
    	{
    		return GroupAggreIterator.hasNext();
    	}
    	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException
    	{
    		return GroupAggreIterator.next();
    	}
    	public void rewind() throws DbException, TransactionAbortedException
    	{
    		GroupAggreIterator = GroupAggre.iterator();
    	}
    	public TupleDesc getTupleDesc()
    	{
    		if(_TupleDesc == null)
    		{
    			if((_gbfield == NO_GROUPING))
    			{
    				Type[] typeAr = {Type.INT_TYPE};
    				return new TupleDesc(typeAr);
    			}
    			else
    			{
    				Type[] typeAr = {_gbfieldtype, Type.INT_TYPE};
    				return new TupleDesc(typeAr);
    			}
    		}
    		else
    			return _TupleDesc;
    	}
    	public void close()
    	{
    		GroupAggreIterator = null;
    	}
    		      
    }
    
    

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	return new StringAggreIterator();
 
    	
 //       throw new UnsupportedOperationException("please implement me for lab2");
    }

}
