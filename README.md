# High-Performance-Spark

## Table of contents:
1. [Chapter 1: Introduction to High Performance Spark](#Chapter1)
2. [Chapter 2: How Spark Works](#Chapter2)
3. [Chapter 3: DataFrames, Datasets, and SparkSQL](#Chapter3)
4. [Chapter 4: Joins (SQL and Core)](#Chapter4)
5. [Chapter 5: Effective Transformations](#Chapter5)
6. [Chapter 6: Working with Key/Value data](#Chapter6)

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
    * preferredLocations(p): Retunrs information about the data locality of a partition p. For example if the RDD represents an HDFS file, it returns the list of the nodes where the data is stored.
    
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
Is the smallest unit of execution representing a local computation. One task can't be executed in more than one executor. the number of tasks per stage corresponds to the number of partitions in the output RDD on that stage. Spark can't run more tasks in parallel than the number of executor cores allocated for the application.


## Chapter 3: DataFrames, Datasets, and SparkSQL<a name="Chapter3"></a>
`Datasets` are like RDD with additional schema information used to provide more efficient storage and optimization. `DataFrames` are like `Datasets` of special `Row[T]` objects.

#### Getting Started with the SparkSession (Or HiveContext or SQLContext)
SparkSession is the entry point for an SparkSQL application, we can get a new session (or an existing one if it exists) calling `SparkSession.getBuilder().getOrCreate()`, calling `enableHiveSupport()` (which is a shortcut for `config(key,value)`)will configure the classpath to use Hive. Prior to Spark 2.0 we had to use `HiveContext` and `SQLContext`, `HiveContext` is preferred as it has better parser and UDF support. To enable Hive support you need both _spark-sql_ and _spark-hive_ dependencies. The SparkSession supports JSON format, it can be loaded using `session.read.json(path)`.

#### Basic of Schemas
SparkSQL can infer schemas on loading, print it (`.printSchema()`), but it is also possible to create the schema programmatically using `StructType`.

#### DataFrame API
You don't need to register temporary tables to work with dataframes. Transformations in Dataframes uses restricted syntax expressions that the optimizer is able to inspect. Dataframes acceps SparkSQL expressions instead of lambdas, columns are accessed with the `apply` function. Dataframes provide operators defined on the column class like `and` to act in more than one column: `df.filter(df("column").and(df("column2") > 0))`. There is a lot of functions defined in the package `org.apache.spark.sql.functions`. `coalesce(col1, col2,...)` returns the first non null column, `nanv1(col1, col2,...)` returns the first non-Nan value. `na` helps handle missing data. Other useful options are `dropDuplicates()` which can drop also duplicates based on a subset of the columns.
The `groupBy` function returns an special object `Dataset` which can be of type `GroupedDataset`, `KeyValueGroupedDataset` and `RelationalGroupedDataset`. the `GroupedDataset` has functionality like `min`, `max`, `avg` and `sum`. We can collect basic stats on a `Dataframe` calling the `describe` method on it, which computes `count`, `mean`, `stdev` and more. To compute more complex aggregations use the `agg` API, which accepts a list of aggregate expressions, a String representing the aggregation or a map of column names to aggregate function names.
Windows can also be defined to compute over ranges, you need to specify the rows the window is over, the order of the rows within the window, and the size of the window. Dataframes support sorting, limit the number of results. MultiDataFrame operations are supported `unionAll`, `intersec`, `except`, `distinct`.
If Spark is connected to a Hive metastore, it is possible to run SQL queries `SQLContext.sql(<the query>)`, it is also possible to run queries over parquet files like `SQLContext.sql("Select * from parquet.<path_to_parquet_file>")`.

#### Data Representation in DataFrames and Datasets
`DataFrames` and `Datasets` have columnar cache format and columnar storage format with Kryo serialization, storage optimizations and evaluates lazily (the constructed lineage its called logical plan). _Tungsten_ its a spark component that provides efficiency on SparkSQL as it works on the byte level.

#### Data Loading and Saving Functions
Spark supports multiple direct loading formats like: 
    
    * JSON: Infers schema loading a sample of the data, this operation it's costly
    * JDBC: Represent a natural SQL source. Specific jars needs to be in the classpath, but sparks uses an abstraction `JdbcDialects` to speak with the different vendors. The `save()` method does not required a path at the information is already available.
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
    
For example `df.write.mode(SaveMode.Append).save("outputPath/")`. If you know how consumers would read your data it is beneficial to partition it based on that. When reading partitioned data you point spark to the root directory and it will automatically discover the partitions (only string and numerics can be used as partition keys). Use the `partitionBy(col1, col2,...)` function of `DataFrame`:
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
SparkSQL a JDBC server to allow access to external systems to its resources. This JDBC server is based on the HiveServer2. To start and stop this server from the command line use: `./sbin/start-thriftserver.sh` and `./sbin/stop-thriftserver.sh`. You can set config parameters using `--hiveconf <key=value>`. To Start it programmatically you can create the server with `HiveTriftServer2.startWithContext(hiveContext)`.


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
Preserving the RDD type is important because many transformations are only defined on RDDs of a particular type (max, min and sum can only be performed on numbers). One of the techniques to use to preserve the type is to define subroutines with input and output types defined. The type can be lost when working with `DataFrame` as `RDD` as in this transformation the schema is thrown away(althought you can store the schema in a variable to apply it after).

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
For set up operations like open a DB connection, it is recommended to do the setup at the map partition, and then the transformations using the iterator functions. Spark has two type of shared variables, broadcast variables (written in driver) and accumulators (written in workers).
Broadcast variables are written on the driver and a copy of it is sent to each machine (not one per task) for example a small table or a prediction model:
```scala
    sc.broadcast(theVariable) //Set variable
    rdd.filter(x => x== theVariable.value) //theVariable is a wrapper, to get the value call the value method
```
If a setup can be serialized, a broadcast variable with `transient lazy val` modifiers can be used. Use `unpersist` to remove explicity a broadcasted variable.
Accumulators are used to collect by-product information from an action (computation) and then bring the result to the driver. If the computation takes times multiple times then the accumulator would also be update multiple times. The accumulator operation is associative, you can create new accumulator types by implementing `AccumulatorV2[InputType, ValueType]` and provide `reset` (sets the initial value in the accumulator), `copy` (returns a copy of the accumulator with the same value), `isZero` (is initial value), `value` (returns the value), `merge` (merges two accumulatos) and `add` (adds two accumulators) methods. The input and value types are different because the value method can perform complex computations and return a different value.

### Reusing RDDs
The typical use case for reusing an RDD is using it multiple times, performing multiple actions in the same RDD and long chain of expensive transformations. 

    * Iterative computations: For transformations that uses the same parent RDD multiple times, persisting the RDD would keep it loaded in memory, speeding up the computations.
    * Multiple actions on the same RDD: Each action on an RDD launches its own spark job, so the lineage would be calculated again for each job. We can avoid this by persisting the RDD.
    * If the cost of computing each partition is very high: Storing the intermediate results can reduce the cost of failures. Checkpointing and persisting breaks the computation lineage, so each of the task to recompute will be smaller
    
Persisting in memory is done in the executor JVM is expensive (it has to serialize de-serialize the objects), persisting in disk (like in checkopointing) is also a expensive read and write operation, checkpointing rarely yields performance improvements. Checkpointing prevents transformations with narrow dependencies to be combined in a single task.

#### Persisting and cache
This means materializing the RDD by storing it in memory on the executors to be used during the current job. There is 5 properties that controls each storage options passed to the `persist(StorageLevel)`. Calling `cache` is the same as calling `persist()` which uses the default MEMORY_ONLY:

    * useDisk: The partitions that doesn't fit in memory are stored in Disk, all options that contains DISK activates this flag.
    * useMemory:The RDD will be stored in memory or be directly written to disk (only DISK_ONLY sets this flag to false).
    * useOfHeap: The RDD will be stored outside the executor. 
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
