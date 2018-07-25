package com.rubikloud.question1

import com.rubikloud.dto
import com.rubikloud.path_prefix
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Question 1 Part b)
  * Pick any two datasets and join them using Spark's API, demonstrate join using b) DataSet
  */
object DataSetJoin {
  def main(args: Array[String]): Unit = {

    val appName = "Question1_PartB_DataSetJoin"

    lazy val conf = new SparkConf()
      .setAppName(appName)
    lazy val ss = SparkSession.builder().config(conf).getOrCreate()

    // import implicit conversions
    import ss.implicits._


    // read delimited files as DataFrame then convert Row instances to Customer instances to get a DataSet[Customer]
    val customer_ds = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.CUSTOMER_schema)
      .csv(path = s"${path_prefix}DATA/customer.tbl")
      // DataFrame == DataSet[Row] --> DataSet[CUSTOMER]
      .as[dto.CUSTOMER]

    customer_ds.show(5)
    println(customer_ds.getClass.getTypeName)
    println(customer_ds.first().getClass.getTypeName)

    val nation_ds = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.NATION_schema)
      // DataFrame == DataSet[Row] --> DataSet[NATION]
      .csv(path = s"${path_prefix}DATA/nation.tbl")
      .as[dto.NATION]

    val joined = nation_ds.join(customer_ds, customer_ds.col("C_NATIONKEY") === nation_ds.col("N_NATIONKEY"))
    joined.show()

  }
}
