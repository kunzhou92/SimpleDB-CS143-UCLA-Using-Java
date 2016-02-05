package simpledb;

import java.io.*;
import java.nio.file.NoSuchFileException;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

	private File HFfile;
	private TupleDesc HFtd;
	
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	HFfile = f;
    	HFtd = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return HFfile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
    	return HFfile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return HFtd;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid){
        // some code goes here
    	
    	int offset = pid.pageNumber() * BufferPool.getPageSize();
    	byte[] readin = new byte[BufferPool.getPageSize()];
    	try
    	{
    		RandomAccessFile Input = new RandomAccessFile(HFfile, "r");
    		Input.skipBytes(offset);
    		Input.read(readin);
    		Input.close();	
    		return new HeapPage((HeapPageId)pid, readin);
    	} catch(FileNotFoundException e)
    	{
    		e.printStackTrace();
    	}catch(IOException e)
    	{
    		e.printStackTrace();
    	}
    	return null;
  
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    	int offset = page.getId().pageNumber() * BufferPool.getPageSize();
    	RandomAccessFile output = new RandomAccessFile(HFfile, "rw");
    	output.skipBytes(offset);
    	output.write(page.getPageData(), 0, BufferPool.getPageSize() );
    	output.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)Math.ceil(HFfile.length() * 1.0 / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
    	// not necessary for lab1
    	HeapPageId tempPageId = null;
    	HeapPage tempPage = null;
    	boolean find = false;
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	int PageNum = numPages();
    	for(int i=0; i<PageNum; i++)
    	{
    		tempPageId = new HeapPageId(getId(), i);
    		tempPage = (HeapPage)Database.getBufferPool().getPage(
    					tid, tempPageId, Permissions.READ_WRITE);
    	    if(tempPage.getNumEmptySlots() > 0)
    	    {
    	    	find = true;
    	    	break;
    	    }  	
    	}
    	if(!find)
    	{
    		tempPage = new HeapPage(new HeapPageId(getId(), numPages())
    				, HeapPage.createEmptyPageData());
    	}	
    	tempPage.insertTuple(t);
    	writePage(tempPage);
    	pageList.add(tempPage) ;
    	return pageList;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) 
    		throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1	
    	ArrayList<Page> pageList = new ArrayList<Page>();
    	HeapPage deletePage = (HeapPage)Database.getBufferPool().getPage(
				tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
    	deletePage.deleteTuple(t);
    	pageList.add(deletePage);
        return pageList;
    }

    class HFDBFileTerator implements DbFileIterator
    {
    	private TransactionId IteraTid;
    	private int PageNum;
    	private HeapPageId IteraHeapPageId;
    	private HeapPage IteraPage;
    	private Iterator<Tuple> IteraIterator;
    	
    	public HFDBFileTerator(TransactionId tid)
    	{
    		IteraTid = tid;
    	}
    	
    	public void open()
    			throws DbException, TransactionAbortedException
    	{
    		PageNum = 0;
    		IteraHeapPageId = new HeapPageId(getId(), PageNum);
    		IteraPage = (HeapPage)Database.getBufferPool().getPage(
    				IteraTid, IteraHeapPageId, Permissions.READ_ONLY);
    		IteraIterator = IteraPage.iterator();		
    	}

    	public boolean hasNext() throws DbException, TransactionAbortedException
    	{
	    	if(IteraIterator == null)
	    		return false;
	    	if(IteraIterator.hasNext())
	    		return true;
	    	if(PageNum < (numPages()-1))
	    	{
	    		int tempNum = PageNum + 1;
	    		HeapPageId tempHeapPageId = new HeapPageId(getId(), tempNum);
	    		HeapPage tempHeapPage = (HeapPage)Database.getBufferPool().getPage(
	    				IteraTid, tempHeapPageId, Permissions.READ_ONLY);
	    		Iterator<Tuple> tempIterator = tempHeapPage.iterator();
	    		if(tempIterator.hasNext())
	    			return true;
	    	}
	    	return false;
	    }
    	public Tuple next()
    			throws DbException, TransactionAbortedException, NoSuchElementException
    	{
    		if(!hasNext())
	    		throw new NoSuchElementException();
	    	if(IteraIterator.hasNext())
	    		return IteraIterator.next();
	    	PageNum++;
	    	IteraHeapPageId = new HeapPageId(getId(), PageNum);
    		IteraPage = (HeapPage)Database.getBufferPool().getPage(
    				IteraTid, IteraHeapPageId, Permissions.READ_ONLY);
    		IteraIterator = IteraPage.iterator();
    		return IteraIterator.next();
    	}
    	
    	public void rewind() throws DbException, TransactionAbortedException
	    {
    		PageNum = 0;
        	IteraHeapPageId = new HeapPageId(getId(), PageNum);
        	IteraPage = (HeapPage)Database.getBufferPool().getPage(
        	IteraTid, IteraHeapPageId, Permissions.READ_ONLY);
        	IteraIterator = IteraPage.iterator();	
	    }
    	
    	public void close()
	    {
	    	PageNum = 0;
	    	IteraHeapPageId = null;
	    	IteraPage = null;
	    	IteraIterator = null;
	    }
    	 
    	    
    }
    
    
    
    
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HFDBFileTerator(tid);
    }

}

