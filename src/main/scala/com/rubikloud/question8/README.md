# Question 8

[Back to root README](../../../../../../README.md)

You are given a Sales Dashboard, it joins 7 tables, runs a bunch of aggregations and produces a report. It is currently taking 12 hours to run. The Spark cluster has 12 worker nodes (16cores, 112GB RAM, 2TB local disk), we are running 1000 executors each with 1g of memory per executor. Driver has 30gigs of memory, dynamic allocation is disabled.
Identify the steps you would do, to improve the overall performance on the Sales Dashboard.

# Discussion

## Sanity checks

- Is cluster shared?  Is another application competing for resources?
- Are master and nodes all in same local area network/vnet?

## Executor Size

Executors have ~400MB of overhead so with 1GB executors an inefficiently large proportion of overall memory is not available for caching etc.  Check if executors are writing to disk.
- try doubling executor size until executors no longer write to disk
- balance CPU and RAM usage

## Caching

Are all the tables read from a raw data source?  What has changed in source data since last run?
- Can we persist the results of expensive joins between runs?
- Can we reduce size of tables being joined by only joining new data?


