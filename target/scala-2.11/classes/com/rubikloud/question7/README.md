# Question 7

[Back to root README](../../../../../../README.md)

Explain your thoughts on how you would ensure that ETL Pipelines are tested in an automated fashion, please identify all the steps/prerequisites required along with the assumptions you make. Recommend tools / procedures, assume we do all testing manually. How do we get a to a number which shows our test coverage is measurable and improving over time.

# Discussion

Assume we are talking about a batch style ETL pipeline. It's reads in input dataset as a snapshot of the source data and generates an output dataset in some data model/format optimized for client queries.
The pipeline's output is entirely determined by the
- state of source data at extract time and
- state of ETL code being executed.
 
Neither, one or both of these will change between a "run" of the pipeline and cause a regression.

