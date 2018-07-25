# Question 4

[Back to root README](../../../../../../README.md)

Dynamic allocation is enabled on the Spark Cluster, data is stored on cloud storage, explain how would you influence how many executors are used by Spark Cluster to execute the job. Demonstrate how will we achieve this, evaluate the pros and cons.

## Discussion

### Static vs dynamic allocation

In static allocation, the number of executors created by the cluster is set when the job starts.  With this approach, each application is given a maximum amount of resources it can use, and holds onto them for its whole duration.

In dynamic allocation, executors are added and removed based on the workload.  This allows a cluster to be shared by more than one application (a pro).  Depending on cloud infrastructure this could also reduce costs (ie are we billed the same whether the Spark cluster is idle are not?).  A con is the extra overhead used allocating and de-allocating executors is expensive - this can be medigated by setting the `spark.dynamicAllocation.minExecutors`, `spark.dynamicAllocation.maxExecutors`, and `spark.dynamicAllocation.initialExecutors` configurations (see here: [Spark Configuration of Dynamic Allocation](https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation)).


## References

- [Spark Configuration of Dynamic Allocation](https://spark.apache.org/docs/latest/configuration.html#dynamic-allocation)
- [Spark Cluster Mode](https://spark.apache.org/docs/latest/cluster-overview.html)
- [Spark Dynamic Allocation](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)