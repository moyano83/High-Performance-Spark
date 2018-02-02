# High-Performance-Spark

## Table of contents:
1. [Chapter 1: Introduction to High Performance Spark](#Chapter1)
2. [Chapter 2: How Spark Works](#Chapter2)
3. [Chapter 3: DataFrames, Datasets, and SparkSQL](#Chapter3)
4. [Chapter 4: Joins (SQL and Core)](#Chapter4)
5. [Chapter 5: Effective Transformations](#Chapter5)
6. [Chapter 6: Working with Key/Value data](#Chapter6)
7. [Chapter 7: Going Beyond Scala](#Chapter7)
8. [Chapter 8: Testing and Validation](#Chapter8)

## Chapter 1: Introduction to High Performance Spark<a name="Chapter1"></a>
Skipped.

## Chapter 2: How Spark Works<a name="Chapter2"></a>
Spark is a computational engine that can be used in combination with storage system such as S3, HDFS or Cassandra, and is usually orchestrated with some cluster management systems like Mesos or Yarn (or Spark in standalone mode).

#### Components
    * Spark core: Spark is build around Resiliant Distributed Datasets concept (RDDs), which is a lazy evaluated, statically typed collection in which transformations can be applied. There are other first-party components to provide enhanced functionality including SparkSQL, SparkMLlib, SparkML, GraphX.  
    * SparkSQL: Defines an API for semi structured data called DataFrames or Datasets.
    * Machine Learning packages: ML and MLlib to create machine learning pipelines.
    * Spark Streaming: Streaming analytics on minibatches of data.
    * GraphX: for graph processing
    
#### Spark Model of Computing: RDDs
An RDD is a distributed, immutable collection comprised by objects called partitions. A spark program is coordinated by the driver program (initiated with some configuration) and computed on the working nodes, the spark execution engine distributes the data among the workers.
An RDD is evaluated lazily (nothing is computed until an action is called on the RDD), and can be stored in memory for faster access. Transformations on an RDD yields a new RDD. Actions trigger the scheduler, which builds a direct acyclic graph (DAG), based on dependencies between RDD transformations.
Spark is fault tolerant, the RDD itself contains all the dependency information to recalculate each partition. In addition, Spark can only fail when an action is called, and because of this spark's stack traces usually appears to fail consistently at the point of the action. It also simplifies a lot of intermediate steps like in the case of Map reduce.
Spark offers 3 types of memory management for data:

    * In memory as deserialized java objects: The faster, but less efficient
    * As serialized data: Converts objects into streams of bytes before sending them to the network. Adds overhead in deserializing the object before using it, more CPU intensive and memory efficient.   
    * On disk: When the RDD is too large to be stored in memory we can use this type, it is the slowest, but more resilient to failures. Sometimes there is no other choice if the RDD is too big.

To define which type of memory management to use, we call the `persist()` method on the RDD (defaults to deserialized in memory java objects).
An RDD can be created in 3 ways: by transforming an existing RDD, from a SparkContext or by converting a DataFrame or Dataset. A SparkContext represents the connection between the cluster and the running spark application, it can create RDD through `parallelize` or `createRDD` methods, or by reading from stable storage like HDFS, text files.
DataFrames and Datasets can be created by using the SparkSQL equivalent of the SparkContext, the SparkSession.
Spark uses 5 main properties to represent an RDD, corresponding to the following 5 methods available for the end user:

    * partitions(): Returns an array of the partition objects that make up the parts of a distributed dataset.
    * iterator(p, parentIters): computes the elements of partition p, given iterators for each of its parent partitions. Not intended to be called by the end user
    * dependencies(): Returns a sequence of dependency objects, which are used by the scheduler to know how this RDD depends on other RDDs.
    * partitioner(): Returns an option with the partitioner object if the RDD has a function between element and partition associated to it.
    * preferredLocations(p): Returns information about the data locality of a partition p. For example if the RDD represents an HDFS file, it returns the list of the nodes where the data is stored.
    
The RDD api defines common functions like `map()` and `collect`. Functions that are present only in certain types of RDD are defined in several classes, and are made available using implicit conversions from the abstract RDD class. You can use `toDebugString` method to find the specific RDD type and will provide a list of parent RDDs.
An RDD has two type of functions defined on it: actions (returns something that is not an RDD )and transformations (returns a new RDD). Every Spark program must have an action that forces the evaluation of the lazy computations. Examples of actions are `saveAsTextFile`, `foreach`...
Transformations falls into two categories: with narrow dependencies and with wide dependencies. 
Narrow transformations are those in which each partition in the child RDD has simple, finite dependencies on partitions on the parent RDD. Partitions of this type depends on one parent or a subset of the parent partition and can be executed in a subset of data without any information of other partitions. 
Wide dependencies requires the data to be partitioned according to the value of their key. If an operation requires a shuffle, then Spark adds a `ShuffledDependency` to the dependency list of the RDD.

#### Spark Job Scheduling
A spark program consist of a driver process with the logic of the program, and executor processes scattered across the cluster. The spark program runs on the driver and sends instructions to the executors. Different Spark applications are scheduled by the cluster manager each one corresponding to one SparkContext. Spark program can run multiple concurrent jobs, each one corresponding to an action called in the RDD. Spark allocates resources to the program statically (finite maximum number of resources on the cluster reserved for the duration of the application), or dynamically (executors are added and removed as needed based on a set of heuristic estimations for resource requirement).
A Spark application is a set of jobs defined by one spark context in the driver program. It is initiated when the `SparkContext` is instantiated. When a program starts each executor has slots for running the tasks needed to complete the job. The sequence of execution is:

    * The driver program pings the cluster manager.
    * The cluster manager launches a series of spark executors (each one on its own JVM).
    * The executors can have different partitions, which can not be splitted among multiple nodes.   

The default Spark scheduler is FIFO, but can also use round robin

#### Anatomy of a Spark Job
Spark "doesn't" do anything until an action is called, when this happens, the scheduler builds an execution graph and launches a Spark job which consists of stages composed of collection of tasks (representing parallel executions) which are steps to transform the data.

###### The DAG
The scheduler uses the RDDs dependencies to construct a DAG of stages for each job. The DAG builds a graph of stages for each job, locations to run each task and passes the information to the `TaskScheduler` which creates a graph of dependencies between partitions.

###### The Job
The edges of the spark execution graph are based on the dependencies between the partitions on each RDD transformations. An operation that returns something different than an RDD can't have any children, therefore is a leaf. As Spark can not add anything to the graph after, it launches a job with the transformations.

###### Stages
A Stage corresponds to a shuffle dependency on a wide transformation in a Spark program. A Stage is the set of computations that can be computed in an executor without the need of communication with other executors or the driver program. As the stages boundaries requires communication with the driver program, the stages associated with a job are usually executed sequentially (or in parallel if they compute something in different RDDs combined in a downstream transformation like `join`).

###### Tasks
Is the smallest unit of execution representing a local computation. One task can't be executed in more than one executor. The number of tasks per stage corresponds to the number of partitions in the output RDD on that stage. Spark can't run more tasks in parallel than the number of executor cores allocated for the application.


## Chapter 3: DataFrames, Datasets, and SparkSQL<a name="Chapter3"></a>
`Datasets` are like RDD with additional schema information used to provide more efficient storage and optimization. `DataFrames` are like `Datasets` of special `Row[T]` objects.

#### Getting Started with the SparkSession (Or HiveContext or SQLContext)
SparkSession is the entry point for an SparkSQL application, we can get a new session (or an existing one if it exists) calling `SparkSession.getBuilder().getOrCreate()`, calling `enableHiveSupport()` (which is a shortcut for `config(key,value)`) will configure the classpath to use Hive. Prior to Spark 2.0 we had to use `HiveContext` and `SQLContext`, `HiveContext` is preferred as it has better parser and UDF support. To enable Hive support you need both _spark-sql_ and _spark-hive_ dependencies. The SparkSession supports JSON format, it can be loaded using `session.read.json(path)`.

#### Basic of Schemas
SparkSQL can infer schemas on loading, print it (`dataframe.printSchema()`), but it is also possible to create the schema programmatically using `StructType`.

#### DataFrame API
You don't need to register temporary tables to work with dataframes. Transformations in Dataframes uses restricted syntax expressions that the optimizer is able to inspect. Dataframes accepts SparkSQL expressions instead of lambdas, columns are accessed with the `apply` function. Dataframes provide operators defined on the column class like `and` to act in more than one column: `df.filter(df("column").and(df("column2") > 0))`. There is a lot of functions defined in the package `org.apache.spark.sql.functions`. `coalesce(col1, col2,...)` returns the first non null column, `nanv1(col1, col2,...)` returns the first non-Nan value. `na` helps handle missing data. Other useful options are `dropDuplicates()` which can drop also duplicates based on a subset of the columns.
The `groupBy` function returns an special object `Dataset` which can be of type `GroupedDataset`, `KeyValueGroupedDataset` and `RelationalGroupedDataset`. the `GroupedDataset` has functionality like `min`, `max`, `avg` and `sum`. We can collect basic stats on a `Dataframe` calling the `describe` method on it, which computes `count`, `mean`, `stdev` and more. To compute more complex aggregations use the `agg` API, which accepts a list of aggregate expressions, a String representing the aggregation or a map of column names to aggregate function names.
Windows can also be defined to compute over ranges, you need to specify the rows the window is over, the order of the rows within the window, and the size of the window. Dataframes support sorting, limit the number of results. MultiDataFrame operations are supported `unionAll`, `intersec`, `except`, `distinct`.
If Spark is connected to a Hive metastore, it is possible to run SQL queries `SQLContext.sql(<the query>)`, it is also possible to run queries over parquet files like `SQLContext.sql("Select * from parquet.<path_to_parquet_file>")`.

#### Data Representation in DataFrames and Datasets
`DataFrames` and `Datasets` have columnar cache format and columnar storage format with Kryo serialization, storage optimizations and evaluates lazily (the constructed lineage its called logical plan). _Tungsten_ its a spark component that provides efficiency on SparkSQL as it works on the byte level.

#### Data Loading and Saving Functions
Spark supports multiple direct loading formats like: 
    
    * JSON: Infers schema loading a sample of the data, this operation it's costly
    * JDBC: Represent a natural SQL source. Specific jars needs to be in the classpath, but sparks uses an abstraction `JdbcDialects` to speak with the different vendors. The `save()` method does not require a path as the information about the table is already available.
    * Parquet: Parquet provides space-efficiency, ability to split across multiple files, compression, nested types... We can load a parquet file by calling `df.format("parquet").load(path)`. 
    * Hive Tables: If hive is configured, we can load tables with `session.read.table("myTable")` and save results to a table with `df.write.saveAsTable("myTable")`.
    * RDDs: Can be converted to dataframes by providing a StructType with the schema definition. DataFrames can be converted to RDDs of Row objects (by calling the `.rdd` method).
    * Local collections: You can create a dataframe from a local collection by calling `session.createDataFrame(seqOfObjects)`.
    * Additional Formats: Some vendors publish their own implementations like Avro, RedShift, CSV. This custom packages can be included by passing the `--packages <package>` argument to the shell or submit.
    
In Spark Core, to save an RDD the directory should not exists, with Spark SQL you can choose the behaviour between this:

    * ErrorIfExists: Throws an exception if the directory exists.
    * Append: Append the data if the target exists.
    * Overwrite: Substitute the data
    * Ignore: Silently skip writting if the target exists.
    
For example `df.write.mode(SaveMode.Append).save("outputPath/")`. If you know how consumers would read your data it is beneficial to partition it based on that. When reading partitioned data you point spark to the root directory and it will automatically discover the partitions (only string and numeric can be used as partition keys). Use the `partitionBy(col1, col2,...)` function of `DataFrame`:
`df.write.partitionBy("column1").format("json").save("output/")`.

#### Datasets
Extension that provides additional compile-time type checking (`DataFrames` are a specialized version of `Datasets`). To convert a `DataFrame` to a `Dataset` use the `as[ElementType]`, where `ElementType` must be a case class or similar (types that Spark can represent). To create a `Dataset` from local collections use `sqlContext.createDataSet(...)`. To create a `Dataset` from an `RDD`, transform it first to a `DataFrame` and then to a `Dataset`. In the same way a `Dataset` has an `.rdd` and `toDF` methods to do the inverse transformation.
`Datasets` mix well with scala and Java, and exposes `filter`,`map`,`flatMap` and `mapPartitions` methods. It also have a typed select: `ds.select($"id".as[Long], $"column1".as[String])`.
`groupBy` on `Dataset` returns a `GroupedDataset` or a `KeyValueGroupedDataset` when grouped with an arbitrary function and a `RelationalGroupedDataset` if grouped with a relational/Dataset DSL expression. You can apply functions to grouped data using the function `mapGroups(...)`.

#### Extending with User-Defined Functions and Aggregate Functions (UDFs and UDAFs)
UDFs and UDAFs can be accessed from inside regular SQL expressions so it can leverage the performance of Catalyst.
To use a UDF you have to register it first like this `sqlContext.udf.register("strLen", (s:String) => s.lenght)` so you can use it after in SQL text. To use UDAFs you need to extend the `UserDefinedAggregateFunction` trait and implement some functions.      
 
#### Query Optimizer
Catalyst is the SparkSQL query optimizer, which takes the query plan and transform it into an execution Plan. Using techniques like pattern matching, the optimizer builds a physical plan based on rule-based and cost-based optimizations. Spark might also use code generation using the _Janino_ library. For very large query plans the optimizer might run into problems, that can be solved by converting the `DataFrame`/`Dataset` to an `RDD`, cache it perform the iterative operations and convert the `RDD` back.

#### JDBC/ODBC Server
SparkSQL has a JDBC server to allow access to external systems to its resources. This JDBC server is based on the HiveServer2. To start and stop this server from the command line use: `./sbin/start-thriftserver.sh` and `./sbin/stop-thriftserver.sh`. You can set config parameters using `--hiveconf <key=value>`. To Start it programmatically you can create the server with `HiveTriftServer2.startWithContext(hiveContext)`.


## Chapter 4: Joins (SQL and Core)<a name="Chapter4"></a>
#### Core Spark Joins
Joins are expensive as they require the keys from each RDD to be in the same partition so they can be combined (the data must be shuffled if the two RDDs don't have the same partitioner or should be colocated if they do). The cost of the join increases with the number of keys to join and the distance the records has to travel.

The default join operation in spark includes only keys present in the two RDDs. In case of multiple values per key, it provides all permutations for value/key (which can cause performance problems in case of duplicate keys). Guidelines:

    * If there is duplicate keys, it might be better to do a `distinct` or `combineByKey` operation prior to the join or use the `cogroup` to handle duplicates.
    * If keys are not present in both RDD, use Outer Join if you don't want to lose data, and filter the data after.
    * In case one RDD has an easy to define subset of keys, might be better to reduce the other rdd before the join to avoid a big suffle of data that would be thrown anyway. (For example it might be possible to filter some rows before joining, avoiding shuffling those rows if we don't need them for the result).
    
Spark needs the data to be join to exist in the same partition, the default implementation of join in spark is the _shuffled hash join_. The default partitioner partitions the second RDD with the same partition than the first to ensure the data is in the same partition. The shuffle can be avoid if:

    * Both RDDs has a known partitioner
    * If one of the dataset can fit in memory so we can use a broadcast hash join
    
To speed up the joins we can use different techniques:
    
    * Assigning a known partitioner: If there is an operation that requires a shuffle (`aggregateByKey` or `reduceByKey`) you can pass a partitioner with the same number of partitions as an explicit argument to the first operation and persist the RDD before the join.
    * Broadcast Hash Join: This join pushes the smaller RDD to each of the worker nodes and does a map-side combine with each of the partitions of the larger RDD. This is recommended if one of the RDDs can fit in memory so it avoids shuffles. To use it, collect the smaller RDD as a map in the driver and use `mapPartitions` to combine elements:
    
        val keyValueMap = smallRDD.collectAsMap()
        bigRDD.sparkContext.broadcast(keyValueMap)
        bigRDD.mapPartitions(iter=>
        keyValueMap.get(...)
 
 #### SQL Joins
 Types of Joins:
 
     * DataFrame Joins: The structure of a join in DataFrames is df.join(otherDF, sqlCondition, joinType). As with RDD non unique keys yields cross product of rows. Joins can be one of "inner", "left_outer", "right_outer", "full_outher", "left_anti" (filtering the left table to get only rows that has a key on the right table) or "left_semi" (filtering the left table to get only rows that does not have a key on the right table).
     * Self Joins: To avoid Column name duplication you need to alias the dataframe to have different names.
     * Broadcast has joins: Equivalent to the RDD broadcast has join. Example: df1.join(broadcast(df2), "key")
     * Datasets Joins: It is done with `joinWith`, which yields a tuple of the different record types. Example: ds1.joinWith(ds2,$"columnInDs1" == $"columnInDs2", left_outer)  
    
## Chapter 5: Effective Transformations<a name="Chapter5"></a>

#### Narrow vs Wide Transformations
Wide transformations are those that requires a shuffle, narrow transformations don't. On shuffling operations, if the data needs to pass from one partition to another, it does so by passing it from the executor A to the driver, and then from the driver to executor B.
Narrow dependencies do not require data to be moved across partitions hence does not require communication with the driver node (each series of narrow transformations can be computed on the same stage of the query execution plan). Stages have to be executed sequentially, thus this limits parallelization (thus the cost of a failure in an RDD with wide dependency is higher). This means that chaining together transformations with wide dependencies increases the risk of having to redo very expensive computations (if the risk is high, it might worth to checkpoint intermediate results of the RDD).
Coalesce is a narrow dependency if it reduces the number of partitions (does not require shuffling), and it is wide transformation if it increases the number of partitions.

#### What type of RDD does your transformation return?
Performance in RDD operations is dependant both on the data type contained in each record and in the underlying RDD type (some RDD stores information about the ordering or locality of the data from previous transformations). 
Preserving the RDD type is important because many transformations are only defined on RDDs of a particular type (max, min and sum can only be performed on numbers). One of the techniques to use to preserve the type is to define subroutines with input and output types defined. The type can be lost when working with `DataFrame` as `RDD` as in this transformation the schema is thrown away(although you can store the schema in a variable to apply it after).

#### Minimizing object creation
There are techniques to reduce the amount of objects created (an hence minimizing GC) like reusing existing objects or using smaller data structures. An example of object reuse is through the scala `this.type` in accumulator objects (although is preferably not to use mutable state in Spark or Scala):

```scala
    //Example function of an accumulator
    def sequenceOp(arguments:String): this.type={
        //some logic
        this
    }
``` 
 
 An example of using smaller data structures is trying to use primitive data types instead of custom classes (Arrays instead of case classes or tuples for example). Converting between different types creates also intermediate objects and should be avoided. 
 
#### Iterator-to-Iterator Transformations with mapPartitions
The RDD `mapPartition` takes a function from an input (records in a partition) Iterator to an output iterator. It is important to write this function in such a way that avoids loading the partition in memory (implicitly converting it to a list). When a transformation takes and returns an iterator without forcing it through another collection it is called Iterator-to-Iterator transformation.
Iterator can only be traversed once (extends the `TraversableOnce` trait), and defines methods like `map`, `flatMap`, `++` (addition), `foldLeft`, `foldRight`, `reduce`, `forall`, `exists`, `next` and `foreach`. Distinct to the Spark transformations, iterator transformations are executed linearly, one element at a time rather than in parallel. 
Transformations using iterators allows Spark to spill data to disk selectively allowing spark to manipulate partitions that are too large to fit in memory on a single executor. Iterator to iterator transformations also avoid unnecessary object creation and intermediate data structures.

#### Set operations
RDDs differs on the mathematical sets in the way it handles duplicates, for example union combines the arguments thus the `union` operation size will be the sum of the two RDDs size. `intersection` and `substract` results can be unexpected as the RDDs might contain duplicates.

#### Reducing Setup Overhead
To set up operations like open a DB connection, it is recommended to do the setup at the map partition, and then the 
transformations using the iterator functions. Spark has two type of shared variables, broadcast variables (written in driver) and accumulators (written in workers). Broadcast variables are written on the driver and a copy of it is sent to each machine (not one per task) for example a small table or a prediction model:

```scala
    sc.broadcast(theVariable) //Set variable
    rdd.filter(x => x== theVariable.value) //theVariable is a wrapper, to get the value call the value method
```

If a setup can be serialized, a broadcast variable with `transient lazy val` modifiers can be used. Use `unpersist` to remove explicitly a broadcasted variable.
Accumulators are used to collect by-product information from an action (computation) and then bring the result to the driver. If the computation takes place  multiple times then the accumulator would also be updated multiple times. The accumulator operation is associative, you can create new accumulator types by implementing `AccumulatorV2[InputType, ValueType]` and provide `reset` (sets the initial value in the accumulator), `copy` (returns a copy of the accumulator with the same value), `isZero` (is initial value), `value` (returns the value), `merge` (merges two accumulators) and `add` (adds two accumulators) methods. The input and value types are different because the value method can perform complex computations and return a different value.

### Reusing RDDs
The typical use case for reusing an RDD is using it multiple times, performing multiple actions in the same RDD and long chain of expensive transformations. 

    * Iterative computations: For transformations that uses the same parent RDD multiple times, persisting the RDD would keep it loaded in memory, speeding up the computations.
    * Multiple actions on the same RDD: Each action on an RDD launches its own spark job, so the lineage would be calculated again for each job. We can avoid this by persisting the RDD.
    * If the cost of computing each partition is very high: Storing the intermediate results can reduce the cost of failures. Checkpointing and persisting breaks the computation lineage, so each of the task to recompute will be smaller
    
Persisting in memory is done in the executor JVM is expensive (it has to serialize de-serialize the objects), persisting in disk (like in checkpointing) is also a expensive read and write operation, checkpointing rarely yields performance improvements and prevents transformations with narrow dependencies to be combined in a single task.

#### Persisting and cache
This means materializing the RDD by storing it in memory on the executors to be used during the current job. There is 5 properties that controls each storage options passed to the `persist(StorageLevel)`. Calling `cache` is the same as calling `persist()` which uses the default MEMORY_ONLY:

    * useDisk: The partitions that doesn't fit in memory are stored in Disk, all options that contains DISK activates this flag.
    * useMemory:The RDD will be stored in memory or will be written to disk (only DISK_ONLY sets this flag to false).
    * useOfHeap: The RDD will be stored outside the executor (for example in S3). 
    * deserialized: The RDD will be stored as deserialized java objects. This is activated with options that contains _SER like MEMORY_ONLY_SER.
    * replication: Integer that controls the number of copies persisted into the cluster (defaults to 1). Options that ends in 2 like DISK_ONLY_2 stores two copies of the data.
    
#### Checkpointing
Writes the RDD to an external storage system, but as opposite to `persist()`, it forgets the lineage of the RDD. The recommendation is to use persist when jobs are slow and checkpoint when jobs are failing. To call `checkpoint` call `setCheckpointDir(directory:String)` from the spark context, then call `checkpoint()` on the RDD.

#### LRU caching
RDDs that are stored in memory or disk remains there for the duration of the spark application until `unpersist()` is called on the RDD or it is evicted due to memory shortage (spark uses Least Recently Used).
Spark writes shuffle files which usually contains all of the records in each input partition sorted by the mapper and usually remains there in the executor local directory for the duration of the application (which might be used to avoid recomputing some RDDs), the files are cleaned up when the RDD they represent is garbage collected.

#### Noisy cluster considerations
The clusters with a high volume of unpredictable traffic (called noisy clusters) are particularly suitable for checkpointing and multiple storage copies, this is particularly true for expensive wide transformations. Spark uses FIFO to queue jobs, but this can be changed to a fair scheduler which uses round robin to allocate resources to jobs. It is also possible to configure pools with different weights to allocate resources. Caching does not prevent accumulators to double count a value if the RDD has to be recomputed as the executor might fail entirely.

## Chapter 6: Working with Key/Value data<a name="Chapter6"></a>
Spark has its own PairRDDFunctions class containing operations defined on RDDs of tuples. The OrderedRDDFunctions class contains the methods for sorting. Operations on key/value pairs can cause:

    * Out-of-memory errors in the driver or executor nodes
    * Shuffle failures
    * “Straggler tasks” or partitions, which are especially slow to compute
    
### Actions on Key/Value Pairs
One way to minimize the number of shuffles in a computation that requires several transformations is to make sure to preserve partitioning across narrow transformations to avoid reshuffling data. Also, by using wide transformations such as reduceByKey and aggregateByKey that can perform map-side reductions and that do not require loading all the records for one key into memory, you can prevent memory errors on the executors and speed up wide transformations, particularly for aggregation operations.
To use the functions available in PairRDD and OrderedRDD types, the keys should have an implicit ordering defined, Spark uses implicit conversion to convert an RDD that meets the PairRDD or OrderedRDD requirements from a generic type to the PairRDD or OrderedRDD type. This implicit conversion requires that the correct library already be imported. Thus, to use Spark’s pairRDDFunctions, you need to have imported the SparkContext.
Actions might return unbounded data to the driver (like in `countByKey`, `countByValue`, `lookUp`, and 
`collectAsMap`) thus it can cause memory issues.
Example of using `groupByKey` to calculate ranked statistics over a group of columns: 

```scala
def findRankStatistics(dataFrame: DataFrame,ranks: List[Long]): Map[Int, Iterable[Double]] = {
require(ranks.forall(_ > 0))
//Map to column index, value pairs
val pairRDD: RDD[(Int, Double)] = mapToKeyValuePairs(dataFrame)
val groupColumns: RDD[(Int, Iterable[Double])] = pairRDD.groupByKey()
groupColumns.mapValues(iter => {
//convert to an array and sort
  iter.toArray.sorted.toIterable.zipWithIndex.flatMap({ case (colValue, index) =>
    if (ranks.contains(index + 1)) Iterator(colValue)
    else Iterator.empty
      })
  }).collectAsMap()
}
```
### Choosing an Aggregation Operation
In general it is better to choose aggregation operations that can do some map-side reduction to decrease the number of records by key before shuffling (e.g., aggregate ByKey or reduceByKey). If the accumulator grows in size with the combine operation, then the operation is likely to fail with a memory error (if the accumulator is the size of all the data for that key).

    * groupByKey: Can run out of memory if a given key has too many values for it. Requires a suffle if the partitioner is not known
    * combineByKey: Combines values with the same key using a different return type. Can run out of memory if the combine routine uses too much memory or garbage collector overhead, or the accumulator for one key becomes too large.
    * aggregateByKey: Similar to the combine, faster than combineByKey since it will perform the merging map-side before sending to a combiner
    * reduceByKey: Combines values with the same key, values has to be of the same type. Similar restrictions than the aggregateByKey
    * foldByKey: Combine values with Key the same key using an associative combine function and a zero value, which can be added to the result an arbitrary number of times. Similar restrictions than the reduceByKey.

### Multiple RDD Operations
#### Co-Grouping
 All of the join operations are implemented using the `cogroup` function, which uses the CoGroupedRDD type. A CoGroupedRDD is created from a sequence of key/value RDDs, each with the same key type. `cogroup` shuffles each of the RDDs so that the items with the same value from each of the RDDs will end up on the same partition and into a single RDD by key. `cogroup` requires that all the records in all of the co-grouped RDDs for one key be able to fit on one partition. `cogroup` can be useful as an alternative to join when joining with multiple RDDs. Rather than doing joins on multiple RDDs with one RDD it is more performant to copartition the RDDs since that will prevent Spark from shuffling the RDD being repeatedly joined.

#### Partitioners and Key/Value Data
An RDD without a known partitioner will assign data to partitions according only to the data size and partition size. For RDDs of a generic record type, repartition and coalesce can be used to simply change the number of partitions that the RDD uses, irrespective of the value of the records in the RDD. Repartition shuffles the RDD with a hash partitioner and the given number of partitions, coalesce is a special repartition that avoids a full shuffle if the desired number of partitions is less than the current number of partitions (if is more then is a repartition with hash). For RDDs of key/value pairs, we can use a function called partitionBy, which takes a partition object rather than a number of partitions and shuffles the RDD with the new partitioner.

#### Using the Spark Partitioner Object
 A partitioner is actually an interface with two methods: numPartitions(number of partitions in the RDD after partitioning it) and getPartition (mapping from a key to the integer index of the partition where the record should be send). Two implementations are provided:
 
     * Hash partitioner: The default one for pair RDD operations (not ordered), defines the partition based on the of the key.
     * Range partitioner: Assigns records whose keys are in the same range to a given partition. Range partitioning is required for sorting since it ensures that by sorting records within a given partition, the entire RDD will be sorted. Creating a RangePartitioner with Spark requires not only a number of partitions, but also the actual RDD to sample in order to determine the appropriate ranges to use (breaking the graph), therefore it is both an action and a transformation.
     * Custom Partitioning: To define a custom partitioning, the following methods needs to be implemented:
       - numPartitions: Returns the number of partitions (greater than 0)
       - getPartition: Method that returns the partition index for a given key
       - equals: An optional method to define equality between partitioners, important to avoid unnecesary suffles if the RDD has used the same partitioner.
       - hashcode: Required if the equals method has been overriden.

### Preserving Partitioning Information Across Transformations
Unless a transformation is known to only change the value part of the key/value pair in Spark, the resulting RDD will not have a known partitioner. Common transformations like `map` of `flatMap` can change the key, and even if the function doesn't change it, the result won't have a known partitioner. `mapValues` changes only the values and preserves the partitioner. The mapPartitions function will also preserve the partition if the preserves Partitioning flag is set to true.

#### Leveraging Co-Located and Co-Partitioned RDDs
Co-located RDDs are RDDs with the same partitioner that reside in the same physical location in memory. RDDs can only be combined without any network transfer if they have the same partitioner and if each of the corresponding partitions in-memory are on the same executor. 
We say that multiple RDDs are co-partitioned if they are partitioned by the same known partitioner. RDDs will be co-partitioned if their partitioner objects are equal, but if the corresponding partitions for each RDD are not in the same physical location.

#### Dictionary of Mapping and Partitioning Functions PairRDDFunctions
    
    * mapValues:Preserves the partitioning of the data for use in future operations. If the input RDD has a known partitioner, the output RDD will have the same partitioner.
    * flatMapValues: Preserves partitioner associated with the input RDD. However, the distribution of duplicate values in the keys may change
    * keys: Returns an RDD with the keys preserving the partitioner.
    * values: Returns an RDD with the values, does not preserve partitioning. Future wide transformations will cause a shuffle even if they have the same partitioner as the input RDD.
    * sampleByKey: Given a map from the keys to the percent of each key to sample, returns a stratified sample of the input RDD, preserving the partitioning of the input data.
    * partitionBy: Takes a partitioner object and partitions the RDD accordingly, it always causes a shuffle.
    
#### Dictionary of OrderedRDDOperations

    * sortByKey: Return an RDD sorted by the key, the default number of partitions is the same as the input RDD.
    * partitionAndSortWithinPartitions: Takes a partitioner and an implicit ordering. Partitions the RDD according to the partitioner and then sorts all the records on each partition according to the implicit ordering. The output partitioner is the same as the partitioner argument.
    * filterByRange: Takes a lower and an upper bound for the keys and returns an RDD of just the records whose key falls in that range, preserving the partitioner. When the RDD has already been partitioned by a range partitioner this is cheaper than a generic filter because it scans only the partitions whose keys are in the desired range.
    
#### Sorting by Two Keys with SortByKey
Spark’s sortByKey does allow sorting by tuples of keys for tuples with two elements like in `indexValuePairs.map((_, null)).sortByKey()`, comparing the first value of the tuple and then the second. 

### Secondary Sort and repartitionAndSortWithinPartitions
Using secondary sort in spark is faster than partitioning and then sorting, the repartitionAndSortWithinPartitions function is a wide transformation that takes a partitioner defined on the argument RDD—and an implicit ordering, which must be defined on the keys of the RDD.
`groupByKey` does not maintain the order of the values within the groups. Spark sorting is also not guaranteed to be stable (preserve the original order of elements with the same value). Repeated sorting is not a viable option:` indexValuePairs.sortByKey.map(_.swap()).sortByKey`

### Straggler Detection and Unbalanced Data
_Stragglers_ are those tasks within a stage that take much longer to execute than the other tasks in that stage. When wide transformations are called on the same RDD, stages must usually be executed in sequence, so straggler tasks may hold up an entire job. 

## Chapter 7: Going Beyond Scala<a name="Chapter7"></a>
Spark supports a range of languages for use on the driver, and an even wider range of languages can be used inside of our transformations on the workers. Generally the non-JVM language binding calls the Java interface for Spark using an RPC mechanism, such as Py4J, passing along a serialized representation of the code to be executed on the worker.
Since it can sometimes be convoluted to call the Scala API from Java, Spark’s Java APIs are mostly implemented in Scala while hiding class tags and implicit conversions. Converting JavaRDD to ScalaRDD have sense to access some functionality that might only be 
available in scala.

### Beyond Scala, and Beyond the JVM
Going outside of the JVM in Spark—especially on the workers—can involve a substantial performance cost of copying data on worker nodes between the JVM and the target language. PySpark connects to JVM Spark using a mixture of pipes on the workers and Py4J, a specialized library for Python/Java interoperability, on the driver. Copying the data from the JVM to Python is done using sockets and pickled bytes, or the most part, Spark Scala treats the results of Python as opaque bytes arrays.
You can register Java/Scala UDFs and then use them from Python. Starting in Spark 2.1 this can be done with the register JavaFunction utility on the sqlContext.

## Chapter 8: Testing and Validation<a name="Chapter8"></a>
### Unit testing
Unit tests are generally faster than integration tests and are frequently used during development, to test the data flow of our Spark job, we will need a SparkContext to create testing RDDs or DStreams with local collections. Try to move individual element operations outside the RDD transformations, to remove the need of the RDD on the test class, avoid anonymous functions inside rdd transformations. A simple way to test transformations is to create a SparkContext, parallelize the input, apply your transformations, and collect the results locally for comparison with the expected value. 
Add `System.clearProperty("spark.driver.port")` on the cleanup routine of your tests so spark this is done so that if we run many Spark tests in sequence, they will not bind to the same port. This will result in an exception trying to bind to a port that is already in use.

#### Streaming
Testing Spark Streaming can be done with queueStream which creates input streams like this:

```scala
def makeSimpleQueueStream(ssc: StreamingContext) = {
val input = List(List("hi"), List("happy pandas", "sad pandas")).map(sc.parallelize(_))
val nonCheckpointableInputDsStream = ssc.queueStream(Queue(input:_*))
}
```

Create an input stream for the provided input sequence. This is done using TestInputStream as queueStreams are not checkpointable.

```scala
private[holdenkarau] def createTestInputStream[T: ClassTag]( sc: SparkContext,
ssc_ : TestStreamingContext,
input: Seq[Seq[T]]): TestInputStream[T] = {
new TestInputStream(sc, ssc_, input, numInputPartitions) }
```

spark-testing-base provides two streaming test base classes: Streaming SuiteBase for transformations and StreamingActionBase for actions.

#### Mocking RDDs
kontextfrei is a Scala-only library that enables to you to write the business logic and test code of your Spark application without depending on RDDs, but using the same API.

#### Getting Test Data
Spark has some built-in components for generating random RDDs in the RandomRDDs object in mllib. There are built-in generator functions for exponential, gamma, log‐Normal, normal, poisson, and uniform distributions as both RDDs of doubles and RDDs of vectors. 

```scala
def generateGoldilocks(sc: SparkContext, rows: Long, numCols: Int): RDD[RawPanda] = {
val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000, size = rows) .map(_.toInt.toString)
val valuesRDD = RandomRDDs.normalVectorRDD( sc, numRows = rows, numCols = numCols)
zipRDD.zip(valuesRDD).map{case (z, v) =>RawPanda(1, z, "giant", v(0) > 0.5, v.toArray) }
}
```

#### Sampling
If it’s available as an option to you, sampling your production data can be a great source of test data. Spark’s core RDD and Pair RDD functionality both support customizable random samples. The simplest method for sampling, directly on the RDD class, is the function sample, which takes withReplacement: Boolean, fraction: Double, seed: Long (optional) => `rdd.sample(withReplacement=false, fraction=0.1)`.
You can directly construct a `PartitionwiseSampleRDD` with your own sampler, provided it implements the `RandomSampler` trait from `org.apache.spark.util.random`. `sampleByKeyExact` and `sampleByKey` take in a map of the percentage for each key to keep allowing you to perform stratified sampling.

```scala
   // 5% of the red pandas, and 50% of the giant pandas
val stratas = Map("red" -> 0.05, "giant" -> 0.50)
rdd.sampleByKey(withReplacement=false, fractions = stratas)
```

DataFrames also have sample and randomSplit available directly on them. If you want to perform stratified sampling on DataFrames, you must convert them to an RDD first.

#### Property Checking with ScalaCheck
ScalaCheck is a property-based testing library for Scala similar to Haskell’s Quick‐ Check. Property-based testing allows you to specify invariants about your code (for example, all of the outputs should have the substring “panda”) and lets the testing library generate different types of test input for you. _sscheck_ and _spark-testing-base_, implement generators for Spark.

```scala
// A trivial property that the map doesn't change the number of elements
test("map should not change number of elements") { val property =
  forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) { rdd => rdd.map(_.length).count() == rdd.count()}
  check(property)
}
```

### Integration Testing
Integration tests can be done in several ways:
    
    * Local Mode: Using smaller versions of our data
    * Docker: Creating a mini cluster using docker containers
    * Yarn Mini-cluster: Hadoop has built-in testing libraries to set up a local YarnCluster, which can be a lighter-weight alternative to even Docker. 

### Verifying Performance
You have access to many of the spark counters for verifying performance in the WebUI, and can get programmatic access to them by registering a SparkListener to collect the information. Spark uses callbacks to provide the metrics, and for performance info we can get most of what we need through onTaskEnd, for which Spark gives us a SparkListenerTaskEnd. This Trait defines a method `def onTaskEnd(taskEnd: SparkListenerTaskEnd)` that can be used to collect metrics.