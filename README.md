# High-Performance-Spark

## Table of contents:

2. [Chapter 2: How Spark Works](#Chapter2)
3. [Chapter 3: DataFrames, Datasets, and SparkSQL](#Chapter3)
4. [Chapter 4: Joins (SQL and Core)](#Chapter4)

## Chapter 2: How Spark Works<a name="Chapter2"></a>

Spark is a computational engine that can be used in combination with storage system such as S3, HDFS or Cassandra, and is usually orchestrated with some cluster manager system like Mesos or Yarn (or Spark in standalone mode).

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
    * On disk: When the RDD is too large to be stored in memory we can use this type, it is the slowest, but more resilient to failures. Sometimes there is no other choice if the RDD is too massive.

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
A spark program consist of a driver process with the logic of the program, and executor processes scattered across the cluster. The spark program runs on the driver and sends instructions to the executors. Different Spark applications are scheduled by the cluster manager each one corresponding to one SparkContext. Spark program can run multiple concurrent jobs, each one corresponding to an action called in the RDD. Spark allocates resources to the program statically (finite maximum number of resources on the cluster reserved for the duration of the application), or dinamically (executors are added and removed as needed based on a set of heuristic estimations for resource requirement).
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
`Datasets` mix well with scala and Java, and exposes `filter`,`map`,`flatMap` and `mapPartitions` methods. It also have a typed select:
`ds.select($"id".as[Long], $"column1".as[String])`.
`groupBy` on `Dataset` returns a `GroupedDataset` or a `KeyValueGroupedDataset` when grouped with an arbitrary function and a `RelationalGroupedDataset` if grouped with a relational/Dataset DSL expression. You can apply functions to grouped data using the function `mapGroups(...)`.

#### Extending with User-Defined Functions and Aggregate Functions (UDFs and UDAFs)
UDFs and UDAFs can be accessed from inside regular SQL expressions so it can leverage the performance of Catalyst.
To use a UDF you have to register it first like this `sqlContext.udf.register("strLen", (s:String) => s.lenght)` so you can use it after in SQL text. To use UDAFs you need to extend the `UserDefinedAggregateFunction` trait and implement some functions.      
 
#### Query Optimizer
Catalyst is the SparkSQL query optimizer, which takes the query plan and transform it into an execution Plan. Using techniques like pattern matching, the optimizer builds a physical plan based on rule-based and cost-based optimizations. Spark might also use code generation used the _Janino_ library. For very large query plans the optimizer might run into challenges, that can be solved by converting the `DataFrame`/`Dataset` to an `RDD`, cache it perform the iterative operations and convert the `RDD` back.

#### JDBC/ODBC Server
SparkSQL a JDBC server to allow access to external systems to its resources. This JDBC server is based on the HiveServer2. To start and stop this server from the command line use: `./sbin/start-thriftserver.sh` and `./sbin/stop-thriftserver.sh`. You can set config parameters using `--hiveconf <key=value>`. To Start it programmatically you can create the server with `HiveTriftServer2.startWithContext(hiveContext)`.

## Chapter 4: Joins (SQL and Core)<a name="Chapter4"></a>