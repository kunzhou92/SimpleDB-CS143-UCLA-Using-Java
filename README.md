# Database System
## UCLA CS 143 Lab Projects, Winter 2016
SimpleDB was written in Java. A set of mostly unimplemented classes and interfaces were provided. I wrote the code for these classes. The directory is: _Simple_Database_System/src/java/simpledb/_ 

### Lab1 
Implemented the classes to manage tuples, namely _Tuple_, *TupleDesc* which support integer and (fixed length) string fields and fixed length tuples. (*Field*, *IntField*, *StringField*, and *Type* had already been implemented.

Implemented the *Catalog* which supported the ability to add a new table, as well as getting information about a particular table. 

Implemented the *BufferPool* constructor and the *getPage()* method withwith least recently used replacement policy

Implemented the access methods, *HeapPage* and *HeapFile* and associated ID classes. Each page in a HeapFile was arranged as a set of slots and a bitmap. Each file consisted of page data arranged consecutively on disk. 

Implemented the operator SeqScan.

### Lab2 
Implemented the operators *Filter* which returned tuples that satisfied a *Predicate* and *Join* whcih joined tuples from its two children.

Implemented *IntegerAggregator* and *StringAggregator*. Here, I wrote the logic that actually computes an aggregate over a particular field across multiple groups in a sequence of input tuples. 

Implemented the *Aggregate* operator. 

Implemented the methods related to tuple insertion, deletion, and page eviction in *BufferPool*. 

Implemented the *Insert* and *Delete* operators. *Insert* and *Delete* implemented *DbIterator*, accepting a stream of tuples to insert or delete and outputting a single tuple with an integer field that indicated the number of tuples inserted or deleted. These operators would need to call the appropriate methods in *BufferPool* that actually modified the pages on disk.
