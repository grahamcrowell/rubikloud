package com.rubikloud.question2

import com.rubikloud.{dto, path_prefix}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SqlJoin {

  def main(args: Array[String]): Unit = {

    val appName = "Question2_SqlJoin"

    lazy val conf = new SparkConf()
      .setAppName(appName)
    lazy val ss = SparkSession.builder().config(conf).getOrCreate()

    // import implicit conversions
    import ss.implicits._

    // read delimited files as DataFrame then convert Row instances to Customer instances to get a DataSet[Customer]
    val customer_ds: Dataset[dto.CUSTOMER] = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.CUSTOMER_schema)
      .csv(path = s"${path_prefix}DATA/customer.tbl")
      // DataFrame == DataSet[Row] --> DataSet[CUSTOMER]
      .as[dto.CUSTOMER]
    customer_ds.createOrReplaceTempView("CUSTOMER")

    // sanity check
    assert(customer_ds.isInstanceOf[Dataset[dto.CUSTOMER]])
    customer_ds.show(5)

    val nation_ds = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.NATION_schema)
      // DataFrame == DataSet[Row] --> DataSet[NATION]
      .csv(path = s"${path_prefix}DATA/nation.tbl")
      .as[dto.NATION]
    nation_ds.createOrReplaceTempView("NATION")

    // sanity check
    assert(nation_ds.isInstanceOf[Dataset[dto.NATION]])
    nation_ds.show(5)


    /**
      * SQL join
      */
    val joined_df: DataFrame = ss.sqlContext.sql("SELECT * FROM NATION JOIN CUSTOMER ON NATION.N_NATIONKEY = CUSTOMER.C_NATIONKEY")

    // sanity check
    assert(joined_df.isInstanceOf[DataFrame])
    nation_ds.show(5)
  }
}
