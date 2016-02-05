package simpledb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;




/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;
    private ArrayList<Tuple> GroupAggre;
    private TupleDesc _TupleDesc;
    private boolean FirstTime = true;
    private ArrayList<Integer> MeanCount = new ArrayList<Integer>();
    private ArrayList<Integer> MeanSum = new ArrayList<Integer>();
  
    
 
    
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
    	_gbfield = gbfield;
    	_gbfieldtype = gbfieldtype;
    	_afield = afield;
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
     *Merge a new tuple into the minimum aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *           the Tuple containing an aggregate field and a group-by field
     *  
     */
    private void mergeTupleIntoGroupByMin(Tuple tup)
    {
    	int MinValue;
		if(_gbfield == NO_GROUPING)
		{
			if(GroupAggre.size() == 0)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				tempTuple.setField(0, new IntField(Value));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(0);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				IntField tempIntField2 =  (IntField)(tempTuple.getField(0));
				MinValue = tempIntField2.getValue();
				if(MinValue > Value)
					tempTuple.setField(0, new IntField(Value));
			}
		}
		else
		{
			int index = findGroup(tup.getField(_gbfield));
			if( index== -1)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				tempTuple.setField(0, tup.getField(_gbfield));
    			tempTuple.setField(1, new IntField(Value));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(index);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				IntField tempIntField2 =  (IntField)(tempTuple.getField(1));
				MinValue = tempIntField2.getValue();
				if(MinValue > Value)
					tempTuple.setField(1, new IntField(Value));	
			}
		}
    }
    
    
    /**
     *Merge a new tuple into the maximum aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *           the Tuple containing an aggregate field and a group-by field
     *  
     */
    private void mergeTupleIntoGroupByMax(Tuple tup)
    {
    	int MaxValue;
		if(_gbfield == NO_GROUPING)
		{
			if(GroupAggre.size() == 0)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				tempTuple.setField(0, new IntField(Value));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(0);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				IntField tempIntField2 =  (IntField)(tempTuple.getField(0));
				MaxValue = tempIntField2.getValue();
				if(MaxValue < Value)
					tempTuple.setField(0, new IntField(Value));
			}
		}
		else
		{
			int index = findGroup(tup.getField(_gbfield));
			if( index== -1)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				tempTuple.setField(0, tup.getField(_gbfield));
    			tempTuple.setField(1, new IntField(Value));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(index);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				IntField tempIntField2 =  (IntField)(tempTuple.getField(1));
				MaxValue = tempIntField2.getValue();
				if(MaxValue < Value)
					tempTuple.setField(1, new IntField(Value));	
			}
		}
    }
    

    /**
     *Merge a new tuple into the summing aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *           the Tuple containing an aggregate field and a group-by field
     *  
     */
    private void mergeTupleIntoGroupBySum(Tuple tup)
    {
    	int Sum;
    	if(_gbfield == NO_GROUPING)
		{
			if(GroupAggre.size() == 0)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				tempTuple.setField(0, new IntField(Value));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(0);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				IntField tempIntField2 =  (IntField)(tempTuple.getField(0));
				Sum = tempIntField2.getValue();
				tempTuple.setField(0, new IntField(Sum + Value));
			}
		}
		else
		{
			int index = findGroup(tup.getField(_gbfield));
			if( index== -1)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				tempTuple.setField(0, tup.getField(_gbfield));
    			tempTuple.setField(1, new IntField(Value));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(index);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				int Value = tempIntField.getValue();
				IntField tempIntField2 =  (IntField)(tempTuple.getField(1));
				Sum = tempIntField2.getValue();
				tempTuple.setField(1, new IntField(Sum + Value));	
			}
		}
    }
    
    
    /**
     *Merge a new tuple into the counted aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *           the Tuple containing an aggregate field and a group-by field
     *  
     */
    private void mergeTupleIntoGroupByCount(Tuple tup)
    {
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
    
    
    
    /**
     *Merge a new tuple into the average aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *           the Tuple containing an aggregate field and a group-by field
     *  
     */
    private void mergeTupleIntoGroupByAvg(Tuple tup)
    {
    	if(_gbfield == NO_GROUPING)
		{
			if(GroupAggre.size() == 0)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				MeanSum.add(tempIntField.getValue());
				MeanCount.add(1);
				tempTuple.setField(0, new IntField(tempIntField.getValue()));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(0);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				MeanSum.set(0, MeanSum.get(0) + tempIntField.getValue());
				MeanCount.set(0, MeanCount.get(0) + 1);
				tempTuple.setField(0, new IntField(MeanSum.get(0) / MeanCount.get(0)));
			}
		}
		else
		{
			int index = findGroup(tup.getField(_gbfield));
			if( index== -1)
			{
				Tuple tempTuple = new Tuple(_TupleDesc);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				MeanSum.add(tempIntField.getValue());
				MeanCount.add(1);
				tempTuple.setField(0, tup.getField(_gbfield));
    			tempTuple.setField(1, new IntField(tempIntField.getValue()));
				GroupAggre.add(tempTuple);
			}
			else
			{
				Tuple tempTuple = GroupAggre.get(index);
				IntField tempIntField = (IntField)(tup.getField(_afield)); 
				MeanSum.set(index, MeanSum.get(index) + tempIntField.getValue());
				MeanCount.set(index, MeanCount.get(index) + 1);
				tempTuple.setField(1, new IntField(MeanSum.get(index) / MeanCount.get(index)));	
			}
		}
    }
    
    
  
    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
    	if(_what.equals(Op.MIN))
    		mergeTupleIntoGroupByMin(tup);
    	else if(_what.equals(Op.MAX))
    		mergeTupleIntoGroupByMax(tup);
    	else if(_what.equals(Op.SUM))
    		mergeTupleIntoGroupBySum(tup);
    	else if(_what.equals(Op.COUNT))
    		mergeTupleIntoGroupByCount(tup);
    	else if(_what.equals(Op.AVG))
    		mergeTupleIntoGroupByAvg(tup);
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
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
    	return new StringAggreIterator();
     //   throw new
      //  UnsupportedOperationException("please implement me for lab2");
    }

}
