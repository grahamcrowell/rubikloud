package com.rubikloud.question6

import com.rubikloud.{dto, path_prefix}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.reflect.io.Path

object IncrementalDelta {
  def main(args: Array[String]): Unit = {

    val appName = "Question6_IncrementalDelta"

    val conf = new SparkConf()
      .setAppName(appName)
      .set("spark.io.compression.codec", "snappy")

      /**
        * New feature: see https://issues.apache.org/jira/browse/SPARK-20236
        */
      .set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    // sanity check: check for snappy
    assert(ss.conf.get("spark.io.compression.codec") == "snappy")
    ss.conf.getAll.foreach((key_value) => println(s"${key_value._1} : ${key_value._2}"))

    // import implicit conversions
    import ss.implicits._

    /**
      * load data from [[com.rubikloud.question5]]
      */
    val partitioned_path = s"${path_prefix}DATA/partitioned_line_item"
    val line_item_ship_date_ds = ss.read
      .parquet(partitioned_path)
      .as[dto.LINEITEM_SHIPDATE]

    // sanity check (see README in DATA folder)
    assert(line_item_ship_date_ds.count() == 2399740)
    println(line_item_ship_date_ds.count)
    line_item_ship_date_ds.printSchema()


    /**
      * read new incremental text file
      */
    val line_item_ds_inc: Dataset[dto.LINEITEM] = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.LINEITEM_schema)
      .csv(path = s"${path_prefix}DATA/lineitem_incremental.tbl")
      // DataFrame == DataSet[Row] --> DataSet[CUSTOMER]
      .as[dto.LINEITEM]

    /**
      * split [[dto.LINEITEM.L_SHIPDATE]] into 3
      * - [[dto.LINEITEM_SHIPDATE.L_ship_year]]
      * - [[dto.LINEITEM_SHIPDATE.L_ship_month]]
      * - [[dto.LINEITEM_SHIPDATE.L_ship_day]]
      */
    val line_item_ship_date_ds_inc = line_item_ds_inc.map(
      (line_item: dto.LINEITEM) => {
        // @TODO optimize me
        dto.LINEITEM_SHIPDATE(
          line_item.L_ORDERKEY,
          line_item.L_PARTKEY,
          line_item.L_SUPPKEY,
          line_item.L_LINENUMBER,
          line_item.L_QUANTITY,
          line_item.L_EXTENDEDPRICE,
          line_item.L_DISCOUNT,
          line_item.L_TAX,
          line_item.L_RETURNFLAG,
          line_item.L_LINESTATUS,
          line_item.L_SHIPDATE,
          line_item.L_SHIPDATE.split("-")(0).toInt,
          line_item.L_SHIPDATE.split("-")(1).toInt,
          line_item.L_SHIPDATE.split("-")(2).toInt,
          line_item.L_COMMITDATE,
          line_item.L_RECEIPTDATE,
          line_item.L_SHIPINSTRUCT,
          line_item.L_SHIPMODE,
          line_item.L_COMMENT
        )
      }
    ).as[dto.LINEITEM_SHIPDATE]


    // sanity check
    println(line_item_ship_date_ds_inc.count)
    line_item_ship_date_ds_inc.printSchema()

        /**
          * save table partitioned by [[dto.LINEITEM_SHIPDATE.L_ship_year]] and [[dto.LINEITEM_SHIPDATE.L_ship_month]]
          * using snappy data compression
          */
    line_item_ship_date_ds_inc
      .write
      .mode(SaveMode.Overwrite)
      .partitionBy("L_ship_year", "L_ship_month")
      .parquet(partitioned_path)
    ////    val partitioned_path = s"${path_prefix}DATA/partitioned_line_item"
    ////    Path(partitioned_path).deleteRecursively()
    //
    //    // save in parquet format
    //    line_item_shit_date_ds
    //      .write
    //      .partitionBy("L_ship_year", "L_ship_month")
    //      .parquet(partitioned_path)

  }
}

