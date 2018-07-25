# Question 7

[Back to root README](../../../../../../README.md)

Explain your thoughts on how you would ensure that ETL Pipelines are tested in an automated fashion, please identify all the steps/prerequisites required along with the assumptions you make. Recommend tools / procedures, assume we do all testing manually. How do we get a to a number which shows our test coverage is measurable and improving over time.

# Discussion

Assume we are talking about a batch style ETL pipeline. It's reads in input dataset as a snapshot of the source data and generates an output dataset in some data model/format optimized for client queries.
The pipeline's output is entirely determined by the
- state of source data at extract time and
- state of ETL code being executed.
 
Neither, one or both of these will change between a "run" of the pipeline and cause a regression.

## Strategy: Test these two sources of change separately

> When debugging the first step is usually to determine if the root cause is source data or ETL logic.

### Source Data Validation

ETL pipeline should generate and log a report summarizing pass/fail of following unit tests. Each unit test validates the shape/profile of the source data. Each unit test includes a "severity" setting (breaks load or hurts load) that determines if load is allowed to continue or should fail.

For each file/table define a unit tests that check 
- Column count
- Delimiter (if file)

For each source column define unit tests that check
- Column name
- Data type 
- Nullness
- Uniqueness

For each lookup and 1..m relationship validate referential integrity.
- Every foreign key value exists as primary key value

Other domain specific checks are also possible
- promotion date < sale date
- sale date < shipment date
- every order needs >= 1 item

> You can quantity coverage by computing the proportion of source tables and columns that have these unit tests defined.

### ETL Pipeline Testing

#### Unit Testing

For each transformation:
- create tool to generate mock data (similar TPC-H) (very small dataset - most be fast locally in dev environment)
- manually edit mocked data so that it exhibits shape/characteristics of transformation's input
- create 2nd copy of mocked data and edit to be expected output
- persist these 2 datasets test case data in some data store

> Find/develop a [DataFrame comparison library](https://github.com/MrPowers/spark-fast-tests)

Design transformations so they do one thing.

#### Regression Testing

Extend analytical engine so that it can be use for detecting regressions between loads.
Define metrics that quantify data quality. For example, diff metric values based current dataset with previous output.  This allows you to define acceptable thresholds (eg "predicted total monthly sales shouldn't charges by more than 20% between loads"). 
 
> An automation tool like Jenkins can be used automate the execution of these metrics.