# Question 8

[Back to root README](../../../../../../README.md)

You are given a Sales Dashboard, it joins 7 tables, runs a bunch of aggregations and produces a report. It is currently taking 12 hours to run. The Spark cluster has 12 worker nodes (16cores, 112GB RAM, 2TB local disk), we are running 1000 executors each with 1g of memory per executor. Driver has 30gigs of memory, dynamic allocation is disabled.
Identify the steps you would do, to improve the overall performance on the Sales Dashboard.
