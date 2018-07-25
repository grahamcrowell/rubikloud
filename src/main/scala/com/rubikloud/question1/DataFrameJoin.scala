package com.rubikloud.question1

import com.rubikloud.{dto, path_prefix}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Question 1 Part a)
  * Pick any two datasets and join them using Spark's API, demonstrate join using a) Dataframe
  */
object DataFrameJoin {
  def main(args: Array[String]): Unit = {

    val appName = "Question1_PartA_DataFrameJoin"

    lazy val conf = new SparkConf()
      .setAppName(appName)
    lazy val ss = SparkSession.builder().config(conf).getOrCreate()

    // read delimited files as DataFrame (in Scala, DataFrame is equivalent to DataSet[Row])
    val customer_df = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.CUSTOMER_schema)
      .csv(path = s"${path_prefix}DATA/customer.tbl")

    customer_df.show(5)
    println(customer_df.getClass.getTypeName)
    println(customer_df.first().getClass.getTypeName)

    val nation_df = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.NATION_schema)
      .csv(path = s"${path_prefix}DATA/nation.tbl")

    // join DataFrames on NATIONKEY column using DataFrame API
    val joined = nation_df.join(customer_df, customer_df.col("C_NATIONKEY") === nation_df.col("N_NATIONKEY"))
    joined.show()

  }
}
