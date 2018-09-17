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
9. [Chapter 9: Spark MLlib and ML](#Chapter9)
10. [Chapter 10: Spark Components and Packages](#Chapter10)
11. [Appendix A: Tuning, Debugging, and Other Things Developers Like to Pretend Don’t Exist](#AppendixA)

## Chapter 1: Introduction to High Performance Spark<a name="Chapter1"></a>
Skipped.

## Chapter 2: How Spark Works<a name="Chapter2"></a>
Spark is a computational engine that can be used in combination with storage system such as S3, HDFS or Cassandra, 
and is usually orchestrated with some cluster management systems like Mesos or Yarn (or Spark in standalone mode).

#### Components
    * Spark core: Spark is build around Resiliant Distributed Datasets concept (RDDs), which is a lazy evaluated, 
    statically typed collection in which transformations can be applied. There are other first-party components to 
    provide enhanced functionality including SparkSQL, SparkMLlib, SparkML, GraphX.  
    * SparkSQL: Defines an API for semi structured data called DataFrames or Datasets.
    * Machine Learning packages: ML and MLlib to create machine learning pipelines.
    * Spark Streaming: Streaming analytics on minibatches of data.
    * GraphX: for graph processing
    
#### Spark Model of Computing: RDDs
An RDD is a distributed, immutable collection comprised by objects called partitions. A spark program is coordinated
 by the driver program (initiated with some configuration) and computed on the working nodes, the spark execution 
 engine distributes the data among the workers.
An RDD is evaluated lazily (nothing is computed until an action is called on the RDD), and can be stored in memory 
for faster access. Transformations on an RDD yields a new RDD. Actions trigger the scheduler, which builds a direct 
acyclic graph (DAG), based on dependencies between RDD transformations.
Spark is fault tolerant, the RDD itself contains all the dependency information to recalculate each partition. In 
addition, Spark can only fail when an action is called, and because of this spark's stack traces usually appears to 
fail consistently at the point of the action. It also simplifies a lot of intermediate steps like in the case of Map
 reduce. Spark offers 3 types of memory management for data:

    * In memory as deserialized java objects: The faster, but less efficient
    * As serialized data: Converts objects into streams of bytes before sending them to the network. Adds overhead 
    in deserializing the object before using it, more CPU intensive and memory efficient.   
    * On disk: When the RDD is too large to be stored in memory we can use this type, it is the slowest, but more 
    resilient to failures. Sometimes there is no other choice if the RDD is too big.

To define which type of memory management to use, we call the `persist()` method on the RDD (defaults to 
deserialized in memory java objects).
An RDD can be created in 3 ways: by transforming an existing RDD, from a SparkContext or by converting a DataFrame 
or Dataset. A SparkContext represents the connection between the cluster and the running spark application, it can 
create RDD through `parallelize` or `createRDD` methods, or by reading from stable storage like HDFS, text files.
DataFrames and Datasets can be created by using the SparkSQL equivalent of the SparkContext, the SparkSession.
Spark uses 5 main properties to represent an RDD, corresponding to the following 5 methods available for the end user:

    * partitions(): Returns an array of the partition objects that make up the parts of a distributed dataset.
    * iterator(p, parentIters): computes the elements of partition p, given iterators for each of its parent 
    partitions. Not intended to be called by the end user
    * dependencies(): Returns a sequence of dependency objects, which are used by the scheduler to know how this RDD
     depends on other RDDs.
    * partitioner(): Returns an option with the partitioner object if the RDD has a function between element and 
    partition associated to it.
    * preferredLocations(p): Returns information about the data locality of a partition p. For example if the RDD 
    represents an HDFS file, it returns the list of the nodes where the data is stored.
    
The RDD api defines common functions like `map()` and `collect`. Functions that are present only in certain types of
 RDD are defined in several classes, and are made available using implicit conversions from the abstract RDD class. 
 You can use `toDebugString` method to find the specific RDD type and will provide a list of parent RDDs.
An RDD has two type of functions defined on it: actions (returns something that is not an RDD )and transformations 
(returns a new RDD). Every Spark program must have an action that forces the evaluation of the lazy computations. 
Examples of actions are `saveAsTextFile`, `foreach`...
Transformations falls into two categories: with narrow dependencies and with wide dependencies. 
Narrow transformations are those in which each partition in the child RDD has simple, finite dependencies on 
partitions on the parent RDD. Partitions of this type depends on one parent or a subset of the parent partition and 
can be executed in a subset of data without any information of other partitions. 
Wide dependencies requires the data to be partitioned according to the value of their key. If an operation requires 
a shuffle, then Spark adds a `ShuffledDependency` to the dependency list of the RDD.

#### Spark Job Scheduling
A spark program consist of a driver process with the logic of the program, and executor processes scattered across 
the cluster. The spark program runs on the driver and sends instructions to the executors. Different Spark 
applications are scheduled by the cluster manager each one corresponding to one SparkContext. Spark program can run 
multiple concurrent jobs, each one corresponding to an action called in the RDD. Spark allocates resources to the 
program statically (finite maximum number of resources on the cluster reserved for the duration of the application),
 or dynamically (executors are added and removed as needed based on a set of heuristic estimations for resource 
 requirement).
A Spark application is a set of jobs defined by one spark context in the driver program. It is initiated when the 
`SparkContext` is instantiated. When a program starts each executor has slots for running the tasks needed to 
complete the job. The sequence of execution is:

    * The driver program pings the cluster manager
    * The cluster manager launches a series of spark executors (each one on its own JVM)
    * The executors can have different partitions, which can not be splitted among multiple nodes

The default Spark scheduler is FIFO, but can also use round robin

#### Anatomy of a Spark Job
Spark "doesn't" do anything until an action is called, when this happens, the scheduler builds an execution graph 
and launches a Spark job which consists of stages composed of collection of tasks (representing parallel executions)
 which are steps to transform the data.

###### The DAG
The scheduler uses the RDDs dependencies to construct a DAG of stages for each job. The DAG builds a graph of stages
 for each job, locations to run each task and passes the information to the `TaskScheduler` which creates a graph of
  dependencies between partitions.

###### The Job
The edges of the spark execution graph are based on the dependencies between the partitions on each RDD 
transformations. An operation that returns something different than an RDD can't have any children, therefore is a 
leaf. As Spark can not add anything to the graph after, it launches a job with the transformations.

###### Stages
A Stage corresponds to a shuffle dependency on a wide transformation in a Spark program. A Stage is the set of 
computations that can be computed in an executor without the need of communication with other executors or the driver
 program. As the stages boundaries requires communication with the driver program, the stages associated with a job 
 are usually executed sequentially (or in parallel if they compute something in different RDDs combined in a 
 downstream transformation like `join`).

###### Tasks
Is the smallest unit of execution representing a local computation. One task can't be executed in more than one 
executor. The number of tasks per stage corresponds to the number of partitions in the output RDD on that stage. 
Spark can't run more tasks in parallel than the number of executor cores allocated for the application.


## Chapter 3: DataFrames, Datasets, and SparkSQL<a name="Chapter3"></a>
`Datasets` are like RDD with additional schema information used to provide more efficient storage and optimization. 
`DataFrames` are like `Datasets` of special `Row[T]` objects.

#### Getting Started with the SparkSession (Or HiveContext or SQLContext)
SparkSession is the entry point for an SparkSQL application, we can get a new session (or an existing one if it 
exists) calling `SparkSession.getBuilder().getOrCreate()`, calling `enableHiveSupport()` (which is a shortcut for 
`config(key,value)`) will configure the classpath to use Hive. Prior to Spark 2.0 we had to use `HiveContext` and 
`SQLContext`. `HiveContext` is preferred as it has better parser and UDF support. To enable Hive support you need 
both _spark-sql_ and _spark-hive_ dependencies. The SparkSession supports JSON format, it can be loaded using 
`session.read.json(path)`.

#### Basic of Schemas
SparkSQL can infer schemas on loading, print it (`dataframe.printSchema()`), but it is also possible to create the 
schema programmatically using `StructType`.

#### DataFrame API
You don't need to register temporary tables to work with dataframes. Transformations in Dataframes uses restricted 
syntax expressions that the optimizer is able to inspect. Dataframes accepts SparkSQL expressions instead of 
lambdas, columns are accessed with the `apply` function. Dataframes provide operators defined on the column class 
like `and` to act in more than one column: `df.filter(df("column").and(df("column2") > 0))`. There is a lot of 
functions defined in the package `org.apache.spark.sql.functions`. `coalesce(col1, col2,...)` returns the first non 
null column, `nanv1(col1, col2,...)` returns the first non-Nan value. `na` helps handle missing data. Other useful 
options are `dropDuplicates()` which can drop also duplicates based on a subset of the columns.
The `groupBy` function returns an special object `Dataset` which can be of type `GroupedDataset`, 
`KeyValueGroupedDataset` and `RelationalGroupedDataset`. the `GroupedDataset` has functionality like `min`, `max`, 
`avg` and `sum`. We can collect basic stats on a `Dataframe` calling the `describe` method on it, which computes 
`count`, `mean`, `stdev` and more. To compute more complex aggregations use the `agg` API, which accepts a list of 
aggregate expressions, a String representing the aggregation or a map of column names to aggregate function names.
Windows can also be defined to compute over ranges, you need to specify the rows the window is over, the order of 
the rows within the window, and the size of the window. Dataframes support sorting, limit the number of results. 
MultiDataFrame operations are supported `unionAll`, `intersec`, `except`, `distinct`.
If Spark is connected to a Hive metastore, it is possible to run SQL queries `SQLContext.sql(<the query>)`, it is 
also possible to run queries over parquet files like `SQLContext.sql("Select * from parquet.<path_to_parquet_file>")`.

#### Data Representation in DataFrames and Datasets
`DataFrames` and `Datasets` have columnar cache format and columnar storage format with Kryo serialization, storage 
optimizations and evaluates lazily (the constructed lineage its called logical plan). _Tungsten_ its a spark 
component that provides efficiency on SparkSQL as it works on the byte level.

#### Data Loading and Saving Functions
Spark supports multiple direct loading formats like: 
    
    * JSON: Infers schema loading a sample of the data, this operation it's costly
    * JDBC: Represent a natural SQL source. Specific jars needs to be in the classpath, but sparks uses an 
    abstraction `JdbcDialects` to speak with the different vendors. The `save()` method does not require a path as 
    the information about the table is already available.
    * Parquet: Parquet provides space-efficiency, ability to split across multiple files, compression, nested types.
    We can load a parquet file by calling `df.format("parquet").load(path)`. 
    * Hive Tables: If hive is configured, we can load tables with `session.read.table("myTable")` and save results 
    to a table with `df.write.saveAsTable("myTable")`.
    * RDDs: Can be converted to dataframes by providing a StructType with the schema definition. DataFrames can be 
    converted to RDDs of Row objects (by calling the `.rdd` method).
    * Local collections: You can create a dataframe from a local collection by calling:
    session.createDataFrame(seqOfObjects)
    * Additional Formats: Some vendors publish their own implementations like Avro, RedShift, CSV. This custom 
    packages can be included by passing the `--packages <package>` argument to the shell or submit.
    
In Spark Core, to save an RDD the directory should not exists, with Spark SQL you can choose the behaviour between this:

    * ErrorIfExists: Throws an exception if the directory exists.
    * Append: Append the data if the target exists.
    * Overwrite: Substitute the data
    * Ignore: Silently skip writting if the target exists.
    
For example `df.write.mode(SaveMode.Append).save("outputPath/")`. If you know how consumers would read your data it 
is beneficial to partition it based on that. When reading partitioned data you point spark to the root directory and
 it will automatically discover the partitions (only string and numeric can be used as partition keys). Use the 
 `partitionBy(col1, col2,...)` function of `DataFrame`:
`df.write.partitionBy("column1").format("json").save("output/")`.

#### Datasets
Extension that provides additional compile-time type checking (`DataFrames` are a specialized version of `Datasets`)
. To convert a `DataFrame` to a `Dataset` use the `as[ElementType]`, where `ElementType` must be a case class or 
similar (types that Spark can represent). To create a `Dataset` from local collections use `sqlContext.createDataSet
(...)`. To create a `Dataset` from an `RDD`, transform it first to a `DataFrame` and then to a `Dataset`. In the 
same way a `Dataset` has an `.rdd` and `toDF` methods to do the inverse transformation.
`Datasets` mix well with scala and Java, and exposes `filter`,`map`,`flatMap` and `mapPartitions` methods. It also 
have a typed select: `ds.select($"id".as[Long], $"column1".as[String])`.
`groupBy` on `Dataset` returns a `GroupedDataset` or a `KeyValueGroupedDataset` when grouped with an arbitrary 
function and a `RelationalGroupedDataset` if grouped with a relational/Dataset DSL expression. You can apply 
functions to grouped data using the function `mapGroups(...)`.

#### Extending with User-Defined Functions and Aggregate Functions (UDFs and UDAFs)
UDFs and UDAFs can be accessed from inside regular SQL expressions so it can leverage the performance of Catalyst.
To use a UDF you have to register it first like this `sqlContext.udf.register("strLen", (s:String) => s.lenght)` so 
you can use it after in SQL text. To use UDAFs you need to extend the `UserDefinedAggregateFunction` trait and 
implement some functions.      
 
#### Query Optimizer
Catalyst is the SparkSQL query optimizer, which takes the query plan and transform it into an execution Plan. Using 
techniques like pattern matching, the optimizer builds a physical plan based on rule-based and cost-based 
optimizations. Spark might also use code generation using the _Janino_ library. For very large query plans the 
optimizer might run into problems, that can be solved by converting the `DataFrame`/`Dataset` to an `RDD`, cache it 
perform the iterative operations and convert the `RDD` back.

#### JDBC/ODBC Server
SparkSQL has a JDBC server to allow access to external systems to its resources. This JDBC server is based on the 
HiveServer2. To start and stop this server from the command line use: `./sbin/start-thriftserver.sh` and `
./sbin/stop-thriftserver.sh`. You can set config parameters using `--hiveconf <key=value>`. To Start it 
programmatically you can create the server with `HiveTriftServer2.startWithContext(hiveContext)`.

### Use cases

    * DataFrames can be used when you have primarily relational transformations, which can be extended with UDFs 
    when necessary.
    * Datasets can be used when you want a mix of functional and relational transformations while benefiting from 
    the optimizations for DataFrames and are, therefore, a great alternative to RDDs in many cases. 
    * Pure RDDs work well for data that does not fit into the Catalyst optimizer, although aggregates, complex 
    joins, and windowed operations, can be daunting to express with the RDD API.

## Chapter 4: Joins (SQL and Core)<a name="Chapter4"></a>
#### Core Spark Joins
Joins are expensive as they require the keys from each RDD to be in the same partition so they can be combined (the 
data must be shuffled if the two RDDs don't have the same partitioner or should be colocated if they do). The cost 
of the join increases with the number of keys to join and the distance the records has to travel.

The default join operation in spark includes only keys present in the two RDDs. In case of multiple values per key, 
it provides all permutations for value/key (which can cause performance problems in case of duplicate keys). 
Guidelines:

    * If there is duplicate keys, it might be better to do a `distinct` or `combineByKey` operation prior to the 
    join or use the `cogroup` to handle duplicates.
    * If keys are not present in both RDD, use Outer Join if you don't want to lose data, and filter the data after.
    * In case one RDD has an easy to define subset of keys, might be better to reduce the other rdd before the join 
    to avoid a big suffle of data that would be thrown anyway. (For example it might be possible to filter some rows
     before joining, avoiding shuffling those rows if we don't need them for the result).
    
Spark needs the data to join to exist in the same partition, the default implementation of join in spark is the 
_shuffled hash join_. The default partitioner partitions the second RDD with the same partition than the first to 
ensure the data is in the same partition. The shuffle can be avoid if:

    * Both RDDs has a known partitioner
    * If one of the dataset can fit in memory so we can use a broadcast hash join
    
To speed up the joins we can use different techniques:
    
    * Assigning a known partitioner: If there is an operation that requires a shuffle (`aggregateByKey` or 
    `reduceByKey`) you can pass a partitioner with the same number of partitions as an explicit argument to the 
    first operation and persist the RDD before the join.
    * Broadcast Hash Join: This join pushes the smaller RDD to each of the worker nodes and does a map-side combine 
    with each of the partitions of the larger RDD. This is recommended if one of the RDDs can fit in memory so it 
    avoids shuffles. To use it, collect the smaller RDD as a map in the driver and use `mapPartitions` to combine 
    elements:
    
        val keyValueMap = smallRDD.collectAsMap()
        bigRDD.sparkContext.broadcast(keyValueMap)
        bigRDD.mapPartitions(iter=> keyValueMap.get(...))
 
 #### SQL Joins
 Types of Joins:
 
     * DataFrame Joins: The structure of a join in DataFrames is df.join(otherDF, sqlCondition, joinType). As with 
     RDD non unique keys yields cross product of rows. Joins can be one of "inner", "left_outer", "right_outer", 
     "full_outher", "left_anti" (filtering the left table to get only rows that has a key on the right table) or 
     "left_semi" (filtering the left table to get only rows that does not have a key on the right table).
     * Self Joins: To avoid Column name duplication you need to alias the dataframe to have different names.
     * Broadcast has joins: Equivalent to the RDD broadcast has join. Example: df1.join(broadcast(df2), "key")
     * Datasets Joins: It is done with `joinWith`, which yields a tuple of the different record types. Example: ds1
     .joinWith(ds2,$"columnInDs1" == $"columnInDs2", left_outer)  
    
## Chapter 5: Effective Transformations<a name="Chapter5"></a>

#### Narrow vs Wide Transformations
Wide transformations are those that requires a shuffle, narrow transformations don't. On shuffling operations, if 
the data needs to pass from one partition to another, it does so by passing it from the executor A to the driver, 
and then from the driver to executor B.
Narrow dependencies do not require data to be moved across partitions hence does not require communication with the 
driver node (each series of narrow transformations can be computed on the same stage of the query execution plan). 
Stages have to be executed sequentially, which  limits parallelization (thus the cost of a failure in an RDD with 
wide dependency is higher). This means that chaining together transformations with wide dependencies increases the 
risk of having to redo very expensive computations (if the risk is high, it might worth to checkpoint intermediate 
results of the RDD).
Coalesce is a narrow dependency if it reduces the number of partitions (does not require shuffling), and it is wide 
transformation if it increases the number of partitions.

#### What type of RDD does your transformation return?
Performance in RDD operations is dependant both on the data type contained in each record and in the underlying RDD 
type (some RDD stores information about the ordering or locality of the data from previous transformations). 
Preserving the RDD type is important because many transformations are only defined on RDDs of a particular type 
(max, min and sum can only be performed on numbers). One of the techniques to use to preserve the type is to define 
subroutines with input and output types defined. The type can be lost when working with `DataFrame` as `RDD` as in 
this transformation the schema is thrown away(although you can store the schema in a variable to apply it after).

#### Minimizing object creation
There are techniques to reduce the amount of objects created (an hence minimizing GC) like reusing existing objects 
or using smaller data structures. An example of object reuse is through the scala `this.type` in accumulator objects
 (although is preferably not to use mutable state in Spark or Scala):

```scala
    //Example function of an accumulator
    def sequenceOp(arguments:String): this.type={
        //some logic
        this
    }
``` 
 
 An example of using smaller data structures is trying to use primitive data types instead of custom classes (Arrays
  instead of case classes or tuples for example). Converting between different types creates also intermediate 
  objects and should be avoided. 
 
#### Iterator-to-Iterator Transformations with mapPartitions
The RDD `mapPartition` takes a function from an input (records in a partition) Iterator to an output iterator. It is
 important to write this function in such a way that avoids loading the partition in memory (implicitly converting 
 it to a list). When a transformation takes and returns an iterator without forcing it through another collection it
  is called Iterator-to-Iterator transformation.
Iterator can only be traversed once (extends the `TraversableOnce` trait), and defines methods like `map`, 
`flatMap`, `++` (addition), `foldLeft`, `foldRight`, `reduce`, `forall`, `exists`, `next` and `foreach`. Distinct to
 the Spark transformations, iterator transformations are executed linearly, one element at a time rather than in 
 parallel. 
Transformations using iterators allows Spark to spill data to disk selectively allowing spark to manipulate 
partitions that are too large to fit in memory on a single executor. Iterator to iterator transformations also avoid
 unnecessary object creation and intermediate data structures.

#### Set operations
RDDs differs on the mathematical sets in the way it handles duplicates, for example union combines the arguments 
thus the `union` operation size will be the sum of the two RDDs size. `intersection` and `substract` results can be 
unexpected as the RDDs might contain duplicates.

#### Reducing Setup Overhead
To set up operations like open a DB connection, it is recommended to do the setup at the map partition, and then the
 transformations using the iterator functions. Spark has two type of shared variables, broadcast variables (written 
 in driver) and accumulators (written in workers). Broadcast variables are written on the driver and a copy of it is
  sent to each machine (not one per task) for example a small table or a prediction model:

```scala
    sc.broadcast(theVariable) //Set variable
    rdd.filter(x => x== theVariable.value) //theVariable is a wrapper, to get the value call the value method
```

If a setup can't be serialized, a broadcast variable with `transient lazy val` modifiers can be used. Use 
`unpersist` to remove explicitly a broadcasted variable.
Accumulators are used to collect by-product information from an action (computation) and then bring the result to 
the driver. If the computation takes place  multiple times then the accumulator would also be updated multiple times
. The accumulator operation is associative, you can create new accumulator types by implementing 
`AccumulatorV2[InputType, ValueType]` and provide `reset` (sets the initial value in the accumulator), `copy` 
(returns a copy of the accumulator with the same value), `isZero` (is initial value), `value` (returns the value), 
`merge` (merges two accumulators) and `add` (adds two accumulators) methods. The input and value types are different
 because the value method can perform complex computations and return a different value.

### Reusing RDDs
The typical use case for reusing an RDD is using it multiple times, performing multiple actions in the same RDD and 
long chain of expensive transformations. 

    * Iterative computations: For transformations that uses the same parent RDD multiple times, persisting the RDD 
    would keep it loaded in memory, speeding up the computations.
    * Multiple actions on the same RDD: Each action on an RDD launches its own spark job, so the lineage would be 
    calculated again for each job. We can avoid this by persisting the RDD.
    * If the cost of computing each partition is very high: Storing the intermediate results can reduce the cost of 
    failures. Checkpointing and persisting breaks the computation lineage, so each of the task to recompute will be 
    smaller
    
Persisting in memory is done in the executor JVM and it's expensive (it has to serialize de-serialize the objects), 
persisting in disk (like in checkpointing) is also a expensive read and write operation, checkpointing rarely yields
 performance improvements and prevents transformations with narrow dependencies to be combined in a single task.

#### Persisting and cache
This means materializing the RDD by storing it in memory on the executors to be used during the current job. There 
is 5 properties that controls each storage options passed to the `persist(StorageLevel)`. Calling `cache` is the 
same as calling `persist()` which uses the default MEMORY_ONLY:

    * useDisk: The partitions that doesn't fit in memory are stored in Disk, all options that contains DISK 
    activates this flag.
    * useMemory:The RDD will be stored in memory or will be written to disk (only DISK_ONLY sets this flag to false).
    * useOfHeap: The RDD will be stored outside the executor (for example in S3). 
    * deserialized: The RDD will be stored as deserialized java objects. This is activated with options that 
    contains _SER like MEMORY_ONLY_SER.
    * replication: Integer that controls the number of copies persisted into the cluster (defaults to 1). Options 
    that ends in 2 like DISK_ONLY_2 stores two copies of the data.
    
#### Checkpointing
Writes the RDD to an external storage system, but as opposite to `persist()`, it forgets the lineage of the RDD. The
 recommendation is to use persist when jobs are slow and checkpoint when jobs are failing. To call `checkpoint` call
  `setCheckpointDir(directory:String)` from the spark context, then call `checkpoint()` on the RDD.

#### LRU caching
RDDs that are stored in memory or disk remains there for the duration of the spark application until `unpersist()` 
is called on the RDD or it is evicted due to memory shortage (spark uses Least Recently Used).
Spark writes shuffle files which usually contains all of the records in each input partition sorted by the mapper 
and usually remains there in the executor local directory for the duration of the application (which might be used 
to avoid recomputing some RDDs), the files are cleaned up when the RDD they represent is garbage collected.

#### Noisy cluster considerations
The clusters with a high volume of unpredictable traffic (called noisy clusters) are particularly suitable for 
checkpointing and multiple storage copies, this is particularly true for expensive wide transformations. Spark uses 
FIFO to queue jobs, but this can be changed to a fair scheduler which uses round robin to allocate resources to jobs
. It is also possible to configure pools with different weights to allocate resources. Caching does not prevent 
accumulators to double count a value if the RDD has to be recomputed as the executor might fail entirely.

## Chapter 6: Working with Key/Value data<a name="Chapter6"></a>
Spark has its own PairRDDFunctions class containing operations defined on RDDs of tuples. The OrderedRDDFunctions 
 class contains the methods for sorting. Operations on key/value pairs can cause:

    * Out-of-memory errors in the driver or executor nodes
    * Shuffle failures
    * “Straggler tasks” or partitions, which are especially slow to compute
    
### Actions on Key/Value Pairs
One way to minimize the number of shuffles in a computation that requires several transformations is to make sure to
 preserve partitioning across narrow transformations to avoid reshuffling data. Also, by using wide transformations 
 such as reduceByKey and aggregateByKey that can perform map-side reductions and that do not require loading all the
 records for one key into memory, you can prevent memory errors on the executors and speed up wide transformations,
 particularly for aggregation operations.
To use the functions available in PairRDD and OrderedRDD types, the keys should have an implicit ordering defined, 
 Spark uses implicit conversion to convert an RDD that meets the PairRDD or OrderedRDD requirements from a generic 
 type to the PairRDD or OrderedRDD type. This implicit conversion requires that the correct library already be 
 imported. Thus, to use Spark’s pairRDDFunctions, you need to have imported the SparkContext.
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
In general it is better to choose aggregation operations that can do some map-side reduction to decrease the number 
of records by key before shuffling (e.g., aggregate ByKey or reduceByKey). If the accumulator grows in size with the
 combine operation, then the operation is likely to fail with a memory error (if the accumulator is the size of all 
 the data for that key).

    * groupByKey: Can run out of memory if a given key has too many values for it. Requires a suffle if the 
    partitioner is not known
    * combineByKey: Combines values with the same key using a different return type. Can run out of memory if the 
    combine routine uses too much memory or garbage collector overhead, or the accumulator for one key becomes too large
    * aggregateByKey: Similar to the combine, faster than combineByKey since it will perform the merging map-side 
    before sending to a combiner
    * reduceByKey: Combines values with the same key, values has to be of the same type. Similar restrictions than
    the aggregateByKey
    * foldByKey: Combine values with Key the same key using an associative combine function and a zero value, which 
    can be added to the result an arbitrary number of times. Similar restrictions than the reduceByKey.

### Multiple RDD Operations
#### Co-Grouping
 All of the join operations are implemented using the `cogroup` function, which uses the CoGroupedRDD type. A 
 CoGroupedRDD is created from a sequence of key/value RDDs, each with the same key type. `cogroup` shuffles each of 
 the RDDs so that the items with the same value from each of the RDDs will end up on the same partition and into a 
 single RDD by key. `cogroup` requires that all the records in all of the co-grouped RDDs for one key be able to fit
  on one partition. `cogroup` can be useful as an alternative to join when joining with multiple RDDs. Rather than 
  doing joins on multiple RDDs with one RDD it is more performant to copartition the RDDs since that will prevent 
  Spark from shuffling the RDD being repeatedly joined.

#### Partitioners and Key/Value Data
An RDD without a known partitioner will assign data to partitions according only to the data size and partition size. 
For RDDs of a generic record type, repartition and coalesce can be used to simply change the number of partitions 
that the RDD uses, irrespective of the value of the records in the RDD. Repartition shuffles the RDD with a hash 
partitioner and the given number of partitions, coalesce is a special repartition that avoids a full shuffle if the 
desired number of partitions is less than the current number of partitions (if is more then is a repartition with 
hash). For RDDs of key/value pairs, we can use a function called partitionBy, which takes a partition object rather 
than a number of partitions and shuffles the RDD with the new partitioner.

#### Using the Spark Partitioner Object
 A partitioner is actually an interface with two methods: numPartitions(number of partitions in the RDD after 
 partitioning it) and getPartition (mapping from a key to the integer index of the partition where the record should
  be send). Two implementations are provided:
 
     * Hash partitioner: The default one for pair RDD operations (not ordered), defines the partition based on the 
     hashcode of the key.
     * Range partitioner: Assigns records whose keys are in the same range to a given partition. Range partitioning 
     is required for sorting since it ensures that by sorting records within a given partition, the entire RDD will 
     be sorted. Creating a RangePartitioner with Spark requires not only a number of partitions, but also the actual 
     RDD to sample in order to determine the appropriate ranges to use (breaking the graph), therefore it is both an
      action and a transformation.
     * Custom Partitioning: To define a custom partitioning, the following methods needs to be implemented:
       - numPartitions: Returns the number of partitions (greater than 0)
       - getPartition: Method that returns the partition index for a given key
       - equals: An optional method to define equality between partitioners, important to avoid unnecesary suffles 
       if the RDD has used the same partitioner.
       - hashcode: Required if the equals method has been overriden.

### Preserving Partitioning Information Across Transformations
Unless a transformation is known to only change the value part of the key/value pair in Spark, the resulting RDD 
will not have a known partitioner. Common transformations like `map` of `flatMap` can change the key, and even if 
the function doesn't change it, the result won't have a known partitioner. `mapValues` changes only the values and 
preserves the partitioner. The mapPartitions function will also preserve the partition if the preserves Partitioning
 flag is set to true.

#### Leveraging Co-Located and Co-Partitioned RDDs
Co-located RDDs are RDDs with the same partitioner that reside in the same physical location in memory. RDDs can 
only be combined without any network transfer if they have the same partitioner and if each of the corresponding 
partitions in-memory are on the same executor. 
We say that multiple RDDs are co-partitioned if they are partitioned by the same known partitioner. RDDs will be 
co-partitioned if their partitioner objects are equal, but if the corresponding partitions for each RDD are not in 
the same physical location.

#### Dictionary of Mapping and Partitioning Functions PairRDDFunctions
    
    * mapValues:Preserves the partitioning of the data for use in future operations. If the input RDD has a known 
    partitioner, the output RDD will have the same partitioner.
    * flatMapValues: Preserves partitioner associated with the input RDD. However, the distribution of duplicate 
    values in the keys may change
    * keys: Returns an RDD with the keys preserving the partitioner.
    * values: Returns an RDD with the values, does not preserve partitioning. Future wide transformations will cause
     a shuffle even if they have the same partitioner as the input RDD.
    * sampleByKey: Given a map from the keys to the percent of each key to sample, returns a stratified sample of 
    the input RDD, preserving the partitioning of the input data.
    * partitionBy: Takes a partitioner object and partitions the RDD accordingly, it always causes a shuffle.
    
#### Dictionary of OrderedRDDOperations

    * sortByKey: Return an RDD sorted by the key, the default number of partitions is the same as the input RDD.
    * repartitionAndSortWithinPartitions: Takes a partitioner and an implicit ordering. Partitions the RDD according
     to the partitioner and then sorts all the records on each partition according to the implicit ordering. The 
     output partitioner is the same as the partitioner argument.
    * filterByRange: Takes a lower and an upper bound for the keys and returns an RDD of just the records whose key 
    falls in that range, preserving the partitioner. When the RDD has already been partitioned by a range 
    partitioner this is cheaper than a generic filter because it scans only the partitions whose keys are in the 
    desired range.
    
#### Sorting by Two Keys with SortByKey
Spark’s sortByKey does allow sorting by tuples of keys for tuples with two elements like in:

```scala
indexValuePairs.map((_, null)).sortByKey()
```

comparing the first value of the tuple and then the second. 

### Secondary Sort and repartitionAndSortWithinPartitions
Using secondary sort in spark is faster than partitioning and then sorting, the repartitionAndSortWithinPartitions 
function is a wide transformation that takes a partitioner defined on the argument RDD—and an implicit ordering, 
which must be defined on the keys of the RDD.
`groupByKey` does not maintain the order of the values within the groups. Spark sorting is also not guaranteed to be
 stable (preserve the original order of elements with the same value). Repeated sorting is not a viable option:` 
 indexValuePairs.sortByKey.map(_.swap()).sortByKey`

### Straggler Detection and Unbalanced Data
_Stragglers_ are those tasks within a stage that take much longer to execute than the other tasks in that stage. 
When wide transformations are called on the same RDD, stages must usually be executed in sequence, so straggler 
tasks may hold up an entire job. 

## Chapter 7: Going Beyond Scala<a name="Chapter7"></a>
Spark supports a range of languages for use on the driver, and an even wider range of languages can be used inside 
of our transformations on the workers. Generally the non-JVM language binding calls the Java interface for Spark 
using an RPC mechanism, such as Py4J, passing along a serialized representation of the code to be executed on the 
worker.
Since it can sometimes be convoluted to call the Scala API from Java, Spark’s Java APIs are mostly implemented in 
Scala while hiding class tags and implicit conversions. Converting JavaRDD to ScalaRDD have sense to access some 
functionality that might only be available in scala.

### Beyond Scala, and Beyond the JVM
Going outside of the JVM in Spark—especially on the workers—can involve a substantial performance cost of copying 
data on worker nodes between the JVM and the target language. PySpark connects to JVM Spark using a mixture of pipes
 on the workers and Py4J, a specialized library for Python/Java interoperability, on the driver. Copying the data 
 from the JVM to Python is done using sockets and pickled bytes, or the most part, Spark Scala treats the results of
  Python as opaque bytes arrays (_pickle_ is a python module).
Many of Spark’s Python classes simply exist as wrappers to translate your Python calls to the JVM. You can register 
Java/Scala UDFs and then use them from Python. Starting in Spark 2.1 this can be done with the register JavaFunction
 utility on the sqlContext. 
Arbitrary Java objects can be accessed with `sc._gateway.jvm.[fulljvmclassname]`. If you are working in the Scala 
shell you can use the _--packages_ command-line argument to specify the Maven coordinates of a package you want in 
the shell in order to add libraries that give extra functionality to the spark application. If you’re working in the
 PySpark shell command-line arguments aren’t allowed, so you can instead specify the spark.jars.packages 
 configuration variable. PySpark can be installed using pip. 

### How SparkR Works
While a similar PipedRDD wrapper exists for R as it does for Python, it is kept internal and the only public 
interface for working with R is through DataFrames. To execute your own custom R code you can use the _dapply_ 
method on DataFrames, internally _dapply_ is implemented in a similar way to Python’s UDF support.

### Spark.jl (Julia Spark)
The general design of Spark.jl is similar to that of PySpark, with a custom implementation of the PipedRDD that is 
able to parse limited amounts of serialized data from Julia implemented inside of the JVM. The same general 
performance caveats of using PySpark also applies.

### Calling Other Languages from Spark
To use the pipe interface you start by converting your RDDs into a format in which they can be sent over a Unix pipe
. Often simple formats like JSON or CSV are used for communicating, pipe only works with strings, you will need to 
format your inputs as a string and parse the result string back into the correct data type: `someRDD.pipe(SparkFiles
.get(theScriptToCall), enviromentVariablesToPassToTheScript)`.

### JNI
The Java Native Interface (JNI) can be used to interface with other languages like C/C++, write the specification in
 scala or java like this:

```scala
class SumJNI {
  @native def sum(n: Array[Int]): Int
}
```

Once you have your C function and your JNI class specification, you need to generate your class files and from them 
generate the binder heading. The javah command will take the class files and generate headers that is then used to 
create a C-side wrapper. All this boilerplate code can be simplified by using JNA (Java Native Access) instead.


## Chapter 8: Testing and Validation<a name="Chapter8"></a>
### Unit testing
Unit tests are generally faster than integration tests and are frequently used during development, to test the data 
flow of our Spark job, we will need a SparkContext to create testing RDDs or DStreams with local collections. Try to
 move individual element operations outside the RDD transformations, to remove the need of the RDD on the test 
 class, avoid anonymous functions inside rdd transformations. A simple way to test transformations is to create a 
 SparkContext, parallelize the input, apply your transformations, and collect the results locally for comparison 
 with the expected value. 
Add `System.clearProperty("spark.driver.port")` on the cleanup routine of your tests so spark this is done so that 
if we run many Spark tests in sequence, they will not bind to the same port. This will result in an exception trying
 to bind to a port that is already in use.

#### Streaming
Testing Spark Streaming can be done with queueStream which creates input streams like this:

```scala
def makeSimpleQueueStream(ssc: StreamingContext) = {
val input = List(List("hi"), List("happy pandas", "sad pandas")).map(sc.parallelize(_))
val nonCheckpointableInputDsStream = ssc.queueStream(Queue(input:_*))
}
```

Create an input stream for the provided input sequence. This is done using TestInputStream as queueStreams are not 
checkpointable.

```scala
private[thetestpackage] def createTestInputStream[T: ClassTag]( sc: SparkContext,
ssc_ : TestStreamingContext,
input: Seq[Seq[T]]): TestInputStream[T] = {
new TestInputStream(sc, ssc_, input, numInputPartitions) }
```

spark-testing-base provides two streaming test base classes: Streaming SuiteBase for transformations and 
StreamingActionBase for actions.

#### Mocking RDDs
kontextfrei is a Scala-only library that enables to you to write the business logic and test code of your Spark 
application without depending on RDDs, but using the same API.

#### Getting Test Data
Spark has some built-in components for generating random RDDs in the RandomRDDs object in mllib. There are built-in 
generator functions for exponential, gamma, log‐Normal, normal, poisson, and uniform distributions as both RDDs of 
doubles and RDDs of vectors. 

```scala
def generateGoldilocks(sc: SparkContext, rows: Long, numCols: Int): RDD[RawPanda] = {
val zipRDD = RandomRDDs.exponentialRDD(sc, mean = 1000, size = rows) .map(_.toInt.toString)
val valuesRDD = RandomRDDs.normalVectorRDD( sc, numRows = rows, numCols = numCols)
zipRDD.zip(valuesRDD).map{case (z, v) =>RawPanda(1, z, "giant", v(0) > 0.5, v.toArray) }
}
```

#### Sampling
If it’s available as an option to you, sampling your production data can be a great source of test data. Spark’s 
core RDD and Pair RDD functionality both support customizable random samples. The simplest method for sampling, 
directly on the RDD class, is the function sample, which takes withReplacement: Boolean, fraction: Double, seed: 
Long (optional) => `rdd.sample(withReplacement=false, fraction=0.1)`.
You can directly construct a `PartitionwiseSampleRDD` with your own sampler, provided it implements the 
`RandomSampler` trait from `org.apache.spark.util.random`. `sampleByKeyExact` and `sampleByKey` take in a map of the
 percentage for each key to keep allowing you to perform stratified sampling.

```scala
   // 5% of the red pandas, and 50% of the giant pandas
val stratas = Map("red" -> 0.05, "giant" -> 0.50)
rdd.sampleByKey(withReplacement=false, fractions = stratas)
```

DataFrames also have sample and randomSplit available directly on them. If you want to perform stratified sampling 
on DataFrames, you must convert them to an RDD first.

#### Property Checking with ScalaCheck
ScalaCheck is a property-based testing library for Scala similar to Haskell’s QuickCheck. Property-based testing 
allows you to specify invariants about your code (for example, all of the outputs should have the substring “panda”)
 and lets the testing library generate different types of test input for you. _sscheck_ and _spark-testing-base_, 
 implement generators for Spark.

```scala
// A trivial property that the map doesn't change the number of elements
test("map should not change number of elements") { val property =
  forAll(RDDGenerator.genRDD[String](sc)(Arbitrary.arbitrary[String])) {rdd => rdd.map(_.length).count() == rdd.count()}
  check(property)
}
```

### Integration Testing
Integration tests can be done in several ways:
    
    * Local Mode: Using smaller versions of our data
    * Docker: Creating a mini cluster using docker containers
    * Yarn Mini-cluster: Hadoop has built-in testing libraries to set up a local YarnCluster, which can be a 
    lighter-weight alternative to even Docker. 

### Verifying Performance
You have access to many of the spark counters for verifying performance in the WebUI, and can get programmatic access
 to them by registering a SparkListener to collect the information. Spark uses callbacks to provide the metrics, and 
 for performance info we can get most of what we need through onTaskEnd, for which Spark gives us a 
 SparkListenerTaskEnd. This Trait defines a method `def onTaskEnd(taskEnd: SparkListenerTaskEnd)` that can be used to
  collect metrics.

## Chapter 9: Spark MLlib and ML<a name="Chapter9"></a>
MLlib is the first of the two spark machine learning libraries and is entering a maintenance/bug-fix only mode, MLlib
 supports RDDs and ML supports DataFrames and Datasets, they both deal with RDDs and Datasets of vectors. Currently, 
 if you need to do streaming or online training your only option is working with the MLlib APIs, which supports 
 training on streaming data, using the Spark Streaming DStream API.

### Working with MLlib
The Maven coordinates for Spark 2.1’s MLlib are _org.apache.spark:spark-mllib_2.11:2.1.0_. Feature selection and 
scaling require that our data is already in Spark’s internal format. Spark’s internal vector format is distinct from 
Scala’s, and there are separate vector libraries between MLlib and ML, Spark provides a factory object org.apache
.spark.mllib.linalg.Vector, which can construct both dense and sparse vectors: `Vectors.dense(input) //being 
input:Array[Any]`, dense vector can be converted into an sparse one with `toSparse`.
`Word2Vec` and `HashingTF` gets textual data in numeric format, with `HashingTF` operating in an `Iterable[String]` 
and without need for training: `hashingTFObj.transfor(theIterator)`. `Word2Vec` requires training, which is done with
 `word2VecObj.fit(iterableOfStrings)`. Broadcasting the model to transform the RDD can yield a big performance 
 improvement.
To use algorithms on labeled data, create a LabeledPoint with the label and vector of features. LabeledPoint labels 
of doubles, for non-numeric types a custom function or similar techniques (such as StringIndexer) must be used.

    * Feature scaling and selection: Can lead to vastly improved results for certain algorithms and optimizers, MLib 
    provides the `StandardScaler` for scaling, and `ChiSqSelector` for feature selection 
    * Model Training: Most MLlib algorithms present a run, which takes in an RDD of LabeledPoints or Vectors and 
    returns a model
    * Predicting: Most of the MLlib models have predict functions that work on RDDs of SparkVectors, and many also 
    provide an individual predict function for use on one vector at a time, if you need to use the individual predict
     function inside of a transformation, consider broadcasting the model as well
    * Serving and Persistence: MLlib provides export support for two formats, a Spark internal format using 
    _Saveable_ trait (which provides the save and load functions) and PMML (Predictive Model Markup Language) using 
    _PMMLExportable_ trait (wich provides the toPMML function)
    * Model Evaluation: Many models also contain accuracy information, or other summary statistics that can be 
    interesting as a data scientist. _org.apache.spark.mllib.evaluation_ contains tools for calculating different 
    metrics given the predictions and the ground truth.

### Working with Spark ML
The Spark ML API is built around the concept of a “pipeline” consisting of the different stages, data preparation 
tasks and classic model training are available as pipeline stages. The maven coordinates are _org.apache
.spark:spark-mllib_2.11:2.0.0_.  

#### Pipeline Stages
A transformer is the simplest pipeline stage, for transformer pipeline stages, you can directly call the transform 
method, which takes an input DataFrame and returns a new Data Frame. For estimators, you can fit the estimator to a 
particular input DataFrame using fit which returns a transformer pipeline stage that you can then call transform on.
Most of the sates are parameterized, if you are working in the shell, `explainParams()` will list all of the params 
of the stage, the documentation associated with it, and the current value (or `explainParam("paramName")` for a 
single parameter). To set a parameter use `setParameterName([value])` like for example `setIn putCol("inputCol")`.

#### Data Encoding
_org.apache.spark.ml.feature_ package contains algorithms from _Binarizer_, _PCA_ to _Word2Vec_. The most common 
feature transformer is the VectorAssembler, which is used to get your inputs into a format that Spark ML’s machine 
learning models can work with. Spark’s ML pipeline has the basic text-encoding functions like _Word2Vec_, 
_StopWordsRemover_, _NGram_, _IDF_, _HashingTF_, and a simple _Tokenizer_. By using _Tokenizer_ together with 
_HashingTF_ you can convert an input text column into a numeric feature that can be used by Spark. When a 
_StringIndexer_ is fit, it returns a _StringIndexerModel_ which has a counterpoint transformer called 
_IndexToString_, which can be used to convert predictions back to the original labels.

#### Data Cleaning
feature engineering can make a huge difference in the performance of your models. You can use _Binarizer_ to 
threshold a specific feature, and _PCA_ to reduce the dimension of your data. Use _Normalizer_ when you are training
 models that work better with normalized features (entire feature vector), or MinMaxScaler (individual column) to 
 easily add normalization of your features to an existing pipeline.
 
#### Spark ML Models
Each family of machine learning algorithms (classification, regression, recommendation, and clustering) are grouped 
by package. The machine learning estimators often have more parameters available for configuration than the data 
preperation estimators, and often take longer to fit.

#### Putting It All Together in a Pipeline
Example of chaining data preparation and training steps:

```scala
    val tokenizer = new Tokenizer()
    tokenizer.setInputCol("name")
    tokenizer.setOutputCol("tokenized_name")
    val hashingTF = new HashingTF()
    hashingTF.setInputCol("tokenized_name")
    hashingTF.setOutputCol("name_tf")
    val assembler = new VectorAssembler()
    assembler.setInputCols(Array("size", "zipcode", "name_tf","attributes"))
    val nb = new NaiveBayes()
    nb.setLabelCol("happy")
    nb.setFeaturesCol("features")
    nb.setPredictionCol("prediction")
    val pipeline = new Pipeline()
    pipeline.setStages(Array(tokenizer, hashingTF, assembler, nb))
    pipeline.transform(someDf)
```
Calling fit() with a specified Data set will fit the pipeline stages in order (skipping transformers) returning a 
pipeline consisting of entirely trained transformers ready to be used to predict inputs. The root _Pipeline_ class 
contains a stages param consisting of an array of the pipeline stages. After training, the resulting PipelineModel 
has an array called stages, consisting of all of the pipeline stages after fitting which can be accessed 
individually like in `val tokenizer2 = pipelineModel.stages(0).asInstanceOf[Tokenizer]`

#### Data Persistence and Spark ML
If your data is not reused outside of the machine learning algorithm, many iterative algorithms will handle their 
own caching, or allow you to configure the persistence level with the intermediateStorageLevel property. Therefore 
in some cases, not explicitly caching your data when working with Spark’s machine learning algorithms can sometimes 
be faster than explicitly caching your input. Built-in model and pipeline persistence options with Spark ML are 
limited to Spark’s internal format.

With _Automated model selection (parameter search)_ Spark can then search across the different parameters to 
automatically select the best parameters, and the model from it, this search is done linearily and no optimization 
is used to narrow the provided search space. The parameters you search over need not be limited to those on a single 
stage either. You can also configure the evaluation metric used to select the best model:

```scala
    val cv = new CrossValidator().setEstimator(pipeline).setEstimatorParamMaps(paramGrid)
    val bestModel = cv.fit(df).bestModel
```

#### Extending Spark ML Pipelines with Your Own Algorithms
To add your own algorithm to a Spark pipeline, you need create either an Estimator or Transformer, both of which 
implement the PipelineStage interface. Use the Transformer interface for algorithms not requiring training. Use the 
Estimator interface for algorithms that do require training.

_Custom transformers_ in addition to the obvious transform or fit function, all pipeline stages need to provide a 
 transformSchema function and a copy constructor, or implement a class that provides these for you. In addition to 
 producing the output schema, the transformSchema function should validate that the input schema is suitable for the
  stage (for example checking the data types). Your pipeline stage can be configurable using the params interface, 
  the two most common parameters are input column and output column.

_Custom estimators_ the primary difference between the Estimator and Transformer interfaces is that rather than 
directly expressing your transformation on the input, you will first have a training step in the form of a train 
function. For many algorithms, the _org.apache.spark.ml.Predictor_ or _org.apache.spark.ml.classificationClassifier_
 helper classes are easier to work with than directly using the Estimator interface. The Predictor interface adds 
 the two most common parameters (input and output columns, as labels column, features column, and prediction column)
  and automatically handles the schema transformation for us. The Classifier interface is similar to the Predictor 
  interface. The predictor interface additionally includes a rawPredictionColumn in the output and provides tools to
   detect the number of classes (getNumClasses), which are helpful for classification problems.


## Chapter 10: Spark Components and Packages<a name="Chapter10"></a>
### Stream Processing with Spark
Spark Streaming has two APIs, one based on RDDs—called DStreams—and a second (currently in alpha)—called Structured 
Streaming—based on Spark SQL/DataFrames. (While developing and testing streaming applications in local mode, a 
common problem can result from launching with local or local[1] as the master. Spark Streaming requires multiple 
workers to make progress, so for tests make sure to use a larger number of workers like local[4].)

#### Sources and Sinks
Both of Spark Streaming’s APIs directly support file sources that will automatically pick up new subdirectories and 
raw socket sources for testing. For simple testing, Spark offers in-memory streams, QueueStreams for DStream and 
MemoryStream for streaming Datasets. The DS Stream API has support for Kafka, Flume, Kinesis, Twitter, ZeroMQ, and 
MQTT.
Many of Spark’s DStream sources depend on dedicated receiver processes to read in data from your streaming sources 
(not Kafka neither file-based sources). In the receiver-based configuration, a certain number of workers are 
configured to read data from the input data streams, for receiver-based approaches, the partitioning of your DStream
 reflects your receiver configuration. In the direct (receiverless) approaches the initial partitioning is based on 
 the partitioning of the input stream. If your data is not ideally partitioned for processing, explicitly 
 repartitioning as we can on RDDs is the simplest way to fix it (when repartitioning keyed data, having a known 
 partitioner can speed up many operations). If the bottleneck comes from reading, you must increase the number of 
 the partitions in the input data source (for direct) or number of receivers: 

```scala
//transform takes the RDD for each time slice and applies your transformations.
def dStreamRepartition[A: ClassTag](dstream: DStream[A]):DStream[A] = stream.transform{rdd=> rdd.repartition(20)}
```

### Batch Intervals
Spark Streaming processes each batch interval completely before starting the next one, you should set your batch 
interval to be high enough for the previous batch to be processed before the next one starts (starts with a high 
value and reduce until you 
find the optimal). In the DStream API the interval is configured on application/context level like in `new 
StreamingContext(sc, Seconds(1))`, for the Structured Streaming API is configured on a per-output/query.

### Data Checkpoint Intervals
As with iterative algorithms, streaming operations can generate DAGs or query plans that are too large for the 
driver program to keep in memory, for operations that depend on building a history, like streaming aggregations on 
Datasets and updateStateByKey on DStreams, check‐pointing is required to prevent the DAG or query plan from growing
 too large:

```scala
sparkContext.setCheckpointDir(directory)
dsStream.checkpoint()
```

### Considerations for DStreams
Most of the operations of the DSStream API are simple wrappers of RDD methods with transform, but not all. The 
window operation allows you to construct a sliding window of time of your input DStream, Window operations allow you
 to compute your data over the last K batches of data. Window operations are defined based on the windowDuration, 
 which is the width of the window, and the slideDuration, which is how often window is computed. To save batch as 
 text files use `dstream.saveAsTextFiles(target)`.
`foreachRDD` works almost the same as transform, except it is an action instead of a transformation:
`dstream.foreachRDD((rdd, window) => rdd.saveAsSequenceFile(target + window))`.

### Considerations for Structured Streaming
Structured Streaming allows you to conceptually think of running SQL queries on an infinite table, which has records
 appended to it by the stream. Streaming keeps the existing Dataset type and adds a boolean isStreaming to 
 difference it between streaming and batch Datasets. Not all operations available in Datasets are present in the 
 Structured Streaming Datasets (like `toJson`).

#### Data sources
To load a streaming data source simply call readStream instead of read: `session.readStream.parquet(inputPath) 
//Sampling schema inference doesn’t work with streaming data`

#### Output operations
An important required configuration is the out putMode, which is unique and can be set to either append (for new 
rows) or complete (for all rows). The DataStreamWriter can write collections out to the following formats: _console_
 (writes the result out to the terminal), _foreach_ (format can't be specified, it must be set by calling foreach on
  the writer object and set up the desired function), and _memory_ (writes the result out to a local table):
```scala
dsStream.writeStream.outputMode(OutputMode.Complete())
.format("parquet").trigger(ProcessingTime(1.second))
.queryName("pandas").start()
```

#### Custom sinks
 A custom sink needs to be supplied by name, so it is difficult to construct it with arbitrary functions (at compile
  time one needs to know the function for the specified sink. e.g., it cannot vary based on user input). An example 
  of this is shown below:
`ds.writeStream.format("com.test.MyCustomSink").queryName("customSink").start()` 

#### Machine learning with Structured Streaming
The machine learning API is not yet integrated in the Structured Streaming, although you can get it work with custom
 work. Spark’s Structured Streaming has an in-memory table output format that you can use to store the aggregate 
 counts:

`ds.writeStream.outputMode(OutputMode.Complete()).format("memory").queryName(myTblName).start()`

You can also can come up with an update mechanism on how to merge new data into your existing model, the DStream 
`foreachRDD` implementation allows you to access the underlying microbatch view of the data, `foreachRDD` doesn’t 
have a direct equivalent in Structured Streaming, but by using a custom sink you can get similar behavior.

#### Stream status and debugging
Each query is associated with at most one sink, but possible multiple sources. The status function on a 
StreamingQuery returns an object containing the status information for the query and all of the data sources 
associated with it.

### High Availability Mode (or Handling Driver Failure or Checkpointing)
High availability mode works by checkpointing the driver state, and allows Spark to recover when the driver program 
fails. To allow the restart of an streaming application to be successful, you need to provide a function to handle 
recovery:

```scala
// Accumulators and broadcast variables are not currently recovered in high availability mode.
def createStreamingContext(): StreamingContext = { 
val ssc = new StreamingContext(sc, Seconds(1))
// Then create whatever stream is required, and whatever mappings need to go on those streams
ssc.checkpoint(checkpointDir)
ssc
}
val ssc = StreamingContext.getOrCreate(checkpointDir,createStreamingContext _)
// Do whatever work needs to be done regardless of state, start context and run
ssc.start()
```

### Creating a Spark Package
Spark Packages allow people outside of the Apache Spark project to release their improvements or libraries built on 
top of Spark. 

## Appendix A: Tuning, Debugging, and Other Things Developers Like to Pretend Don’t Exist<a name="AppendixA"></a>
### Spark Tuning and Cluster Sizing
Most Spark settings can only be adjusted at the application level. These configurations can have a large impact on a
 job’s speed and chance of completing. The primary resources that the Spark application manages are CPU (number of 
 cores) and memory. Generally speaking there are four primary pieces of information we have to know about our hardware:
 
    * How large can one request be? In YARN cluster mode, this is the maximum size of the YARN container.
    * How large is each node? The memory available to each node is likely greater than or equal to one container. 
    * How many nodes does your cluster have? How many are active? In general it is best to have at least one 
    executor per node.
    * What percent of the resources are available on the system where the job will be submitted? If you are using a 
    shared cluster environment, is the cluster busy? Often, if a Spark application requests more resources than 
    exist in the queue into which it is submitted, but not more than exist on the whole cluster, the request will 
    not fail, but instead will hang in a pending state.
    
### Basic Spark Core Settings: How Many Resources to Allocate to the Spark Application?
The size of the driver, size of the executors, and the number of cores associated with each executor, are 
configurable from the conf and static for the duration of a Spark application (all executors should be of equal size).
In YARN cluster mode and YARN client mode the executor memory overhead is set with _the spark.yarn.executor
.memoryOverhead_ value. In YARN cluster mode the driver memory is set with _spark.yarn.driver.memoryOverhead_, but 
in YARN client mode that value is called _spark.yarn.am.memoryOverhead_. Follow the following rule to calculate the 
requirements:
MEMORY OVERHEAD =Max(MEMORY_OVERHEAD_FACTOR x requested memory, MEMORY_OVERHEAD_MINIMUM). Where 
MEMORY_OVERHEAD_FACTOR = 0.10 and MEMORY_OVERHEAD_MINIMUM = 384 mb.
Jobs may fail if they collect too much data to the driver or perform large local computations, increasing the driver
 memory with _spark.driver.maxResultSize_ may prevent the out-of-memory errors in the driver (set to 0 to disregard 
 the limit). In YARN and Mesos cluster mode, the driver can be run with more cores with _spark.driver.cores_, 
 usually the driver requires one core.

#### A Few Large Executors or Many Small Executors?

    * Many small executors: 2 Downsizes: The risk or running out of resources to compute a partition and having too 
    many executors may not be an efficient use of our resources. If resources are available, executors should be no 
    smaller than about four gigs, at which point overhead will be based on the memory overhead factor (a constant 10
     percent of the executor).
    * Many large executors: Very large executors may be wasteful, to use all the resources and to have the driver 
    smaller than the size of one node, we might need to have more executors per node than one, very large executors 
    may cause delays in garbage collection. Experience shows that five cores per executor should be the upper limit. 

#### Allocating Cluster Resources and Dynamic Allocation
Dynamic allocation is a process by which a Spark application can request and de- commission executors as needed 
throughout the course of an application. With dynamic allocation, Spark requests additional executors when there are
 pending tasks. Second, Spark decommissions executors that have not been used to compute a job in the amount of time
  specified by the _spark.dynamicAllocation.executorIdleTime_. By default Spark doesn't decommission nodes with 
  cache data, use _spark.dynamicAllocation.cachedExecutorIdleTimeout_ to set a value for this.
The number of executors that Spark should start with when an application is launched is configured with _spark
.dynamicAllocation.initialExecutors_ (defaults 0). Dynamic allocation does not allow executors to change in size you
 still must determine the size of each executor before starting the job, so size the executors as you would if you 
 were trying to use all the resources on the cluster. For High-traffic clusters, small executors may allow dynamic 
 allocation to increase the number of resources used more quickly if space becomes available on the nodes in a 
 piecemeal way. To configure dynamic allocation:

    * Set the configuration value spark.dynamicAllocation.enabled to true.
    * Configure an external shuffle service on each worker. This varies based on the cluster manager, check Spark 
    documentation for details.
    * Setspark.shuffle.service.enabled to true.
    * Do not provide a value for the spark.executor.instances parameter.

#### Dividing the Space Within One Executor
The JVM size set by the spark.executor.memory property does not include overhead, so Spark executors require more 
space on a cluster than this number would suggest. On an executor, about 25% of memory is reserved for Spark’s 
internal metadata and user data structures, the remaining space on the executor (called M in the Spark 
documentation), is used for execution and storage, and is governed by a fixed fraction, exposed in the conf as the 
_spark.memory.fraction_. Within this M, some space is set aside for storage (Spark’s in-memory storage of 
partitions, whether serialized or not), and the rest can be used for storage or execution. Within the execution 
space, Spark defines a region of the execution called R for storing cached data. The size of R is determined by a 
percentage of M set by the _spark.memory.storageFraction_ (R won't be reclaimed for execution if there is cached 
data present). If an application requires repeated access to an RDD and performance is improved by caching the RDD 
in memory, it may be useful to increase the size of the storage fraction to prevent the RDDs you needed cached from 
being evicted.

#### Number and Size of Partitions
By default, when an RDD is created by reading from stable storage, the number of partitions corresponds to the 
splits configured in that input format. If the number of partitions is not specified for the wide transformation, 
Spark defaults to using the number specified by the conf value of _spark.default.parallelism_. At a minimum you 
should use as many partitions as total cores, increasing the number of partitions can also help reduce out-of-memory
 errors. An strategy to determine the optimal number of partitions can be:

`memory_for_compute (M)<(spark.executor.memory - over head) * spark.memory.fraction`

And if there is cached data:

```text
memory_for_compute (M - R) < (spark.executor.memory - overhead) x 
    spark.memory.fraction x (1 - spark.memory.storage.fraction)
```

Assuming this space is divided equally between the tasks, then each task should not take more than:

`memory_per_task = memory_for_compute / spark.executor.cores`

Thus, we should set the number of partitions to:

`number_of_partitions = size of shuffle stage / memory per task`

We can also try to observe the shuffle stage to determine the memory cost of a shuffle. If we observe that during 
the shuffle stage the computation is not spilling to disk, then it is likely that each partition is fitting 
comfortably in-memory and we don’t need to increase the number of partitions.

#### Serialization Options
Kryo, like the Tungsten serializer, does not natively support all of the same types that are Java serializable. The 
next version of Spark is adding support for Kryo’s unsafe serializer, which can be even faster than Tungsten and can
 be enabled by setting spark.kryo.unsafe to true.
 
#### Some Additional Debugging Techniques
Debugging a system that relies on lazy evaluation requires removing the assumption that the error is necessarily 
directly related to the line of code that appears to trigger the error, adding a take(1) or count call on the parent
 RDDs or DataFrames to track down the issue more quickly in development environments. In production, if the error 
 comes from the executors, you should look for traces like `ERROR Executor: Exception in task 0.0 in stage 0.0 (TID 
 0).` which points you to the line number of the executor code that has failed. In this cases, errors are more 
 easily spotted if there is no use of anonymous functions.

#### Logging
Spark uses the log4j through sl4j logging mechanism internally, to adjust the log level use the sparkContext with 
`sc.setLogLevel(newLevel)` or by using the conf/log4j.properties.template (rename it to log4j.properties). Also, you
 can ship a custom log4j.xml file and provide it to the executors (either by including it in your JAR or with 
 --files) and then add _-Dlog4j.configuration=log4j.xml_ to _spark.executor.extraJavaOptions_ so the executors pick 
 it up.
In YARN, `yarn logs` command can be used to fetch the logs. If you don’t have log aggregation enabled, you can still
 access the logs by keeping them on the worker nodes for a fixed period of time with the _yarn.nodemanager.delete
 .debug-delay-sec_ configuration property.