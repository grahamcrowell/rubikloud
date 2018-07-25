# Question 3

[Back to root README](../../../../../../README.md)

The given dataset above is in raw text storage format. What other data storage format can you suggest to optimize the performance of our Spark workload if we were to frequently scan and read this dataset. Please come up with a code example and explain why you decide to go with this approach. Please note that there's no completely correct answer here. We're interested to hear your thoughts and see the implementation details and performance comparisons.

## Code

See related code in this folder.

## Discussion

Raw text files are slow to read, the first time a new data file is seen by the pipeline persist its contents in partitioned parquet files. Subsequent requests from the pipeline for source data should resolve to the parquet files.

### Efficient access to raw data

Pargquet partitions are similar to [clustered indices in SQL Server](https://docs.microsoft.com/en-us/sql/relational-databases/indexes/clustered-and-nonclustered-indexes-described?view=sql-server-2017) they determine how data physically organized.  But unlike classic SQL databases files with [other distributed data stores](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/bp-partition-key-uniform-load.html), effective partitioning 