package com.rubikloud.question3

import java.nio.file.Files
import java.util.Date

import com.rubikloud.{dto, path_prefix}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.reflect.io.{File, Path}

/** See the README file in this folder.
  * [[DataFormat.getAvgItemCountPerReturn_slow]] is as described in question: text file reread for each request
  * [[DataFormat.getAvgItemCountPerReturn_fast]] is as described in README: caches text file in parquet format
  *
  */
object DataFormat {

  /** Business question: for a given year, what is the average item count per returned order
    *
    * @param ss
    * @param ship_year year
    */
  def getAvgItemCountPerReturn(ss: SparkSession, ship_year: Int, line_item_ds: Dataset[dto.LINEITEM]): Unit = {
    val _ship_year = ship_year

    line_item_ds
      .filter(
        (line_item: dto.LINEITEM) => line_item.L_RETURNFLAG == "R"
      )
      .filter(
        (line_item: dto.LINEITEM) => line_item.L_SHIPDATE.startsWith(ship_year.toString)
      )
      .groupBy(line_item_ds.col("L_ORDERKEY"))
      .count()
      .show()

  }

  /** Load text file as Dataset[[dto.LINEITEM]].
    *
    * @param ss
    * @param ship_year forwarded to [[getAvgItemCountPerReturn]]
    */
  def getAvgItemCountPerReturn_slow(ss: SparkSession, ship_year: Int): Unit = {
    val _ship_year = ship_year
    // import implicit conversions
    import ss.implicits._
    // read from text files
    val line_item_ds: Dataset[dto.LINEITEM] = ss.read
      .option("header", "false")
      .option("delimiter", "|")
      // set schema
      .schema(dto.LINEITEM_schema)
      .csv(path = s"${path_prefix}DATA/lineitem.tbl")
      // DataFrame == DataSet[Row] --> DataSet[CUSTOMER]
      .as[dto.LINEITEM]

    getAvgItemCountPerReturn(ss, _ship_year, line_item_ds)
  }

  /** If parquet folder exists use it, else read from file and save it for next time.
    *
    * @param ss
    * @param ship_year forwarded to [[getAvgItemCountPerReturn]]
    */
  def getAvgItemCountPerReturn_fast(ss: SparkSession, ship_year: Int): Unit = {
    val _ship_year = ship_year
    // import implicit conversions
    import ss.implicits._
    val fast_path = s"DATA/lineitem_parquet"
    val line_item_ds: Dataset[dto.LINEITEM] = if (Path(fast_path).isDirectory) {
      ss.read.parquet(fast_path).as[dto.LINEITEM]
    } else {
      // read from text files
      val line_item_ds: Dataset[dto.LINEITEM] = ss.read
        .option("header", "false")
        .option("delimiter", "|")
        // set schema
        .schema(dto.LINEITEM_schema)
        .csv(path = s"${path_prefix}DATA/lineitem.tbl")
        // DataFrame == DataSet[Row] --> DataSet[CUSTOMER]
        .as[dto.LINEITEM]
      // save in parquet format
      line_item_ds.write.parquet(fast_path)
      // return to caller
      line_item_ds
    }
    getAvgItemCountPerReturn(ss, _ship_year, line_item_ds)
  }

  /** If parquet folder exists use it, else read from file and save it for next time.
    *
    * @param ss
    * @param ship_year forwarded to [[getAvgItemCountPerReturn]]
    */
  def getAvgItemCountPerReturn_faster(ss: SparkSession, ship_year: Int): Unit = {
    val _ship_year = ship_year
    // import implicit conversions
    import ss.implicits._
    val fast_path = s"DATA/lineitem_parquet2"
    val line_item_ds: Dataset[dto.LINEITEM] = if (Path(fast_path).isDirectory) {
      ss.read.parquet(fast_path).as[dto.LINEITEM]
    } else {
      // read from text files
      val line_item_ds: Dataset[dto.LINEITEM] = ss.read
        .option("header", "false")
        .option("delimiter", "|")
        // set schema
        .schema(dto.LINEITEM_schema)
        .csv(path = s"${path_prefix}DATA/lineitem.tbl")
        // DataFrame == DataSet[Row] --> DataSet[CUSTOMER]
        .as[dto.LINEITEM]
      // save in parquet format
      line_item_ds.write
        .partitionBy("L_RETURNFLAG", "L_SHIPDATE")
//        .sortBy("L_ORDERKEY")
        .parquet(fast_path)
      // return to caller
      line_item_ds
    }
    getAvgItemCountPerReturn(ss, _ship_year, line_item_ds)
  }

  def main(args: Array[String]): Unit = {

    val appName = "Question3_DataFormat"

    val conf = new SparkConf()
      .setAppName(appName)
    val ss = SparkSession.builder()
      .config(conf)
      .getOrCreate()

    Path(s"DATA/lineitem_parquet").deleteRecursively()
    Path(s"DATA/lineitem_parquet2").deleteRecursively()

    var before = new Date().toInstant
    getAvgItemCountPerReturn_slow(ss, 1997)
    getAvgItemCountPerReturn_slow(ss, 1995)
    getAvgItemCountPerReturn_slow(ss, 1994)
    var after = new Date().toInstant
    val slow_time = after.getEpochSecond - before.getEpochSecond
    println(s"slow: ${slow_time}")


    before = new Date().toInstant
    getAvgItemCountPerReturn_fast(ss, 1997)
    getAvgItemCountPerReturn_fast(ss, 1995)
    getAvgItemCountPerReturn_fast(ss, 1994)
    after = new Date().toInstant
    val fast_time = after.getEpochSecond - before.getEpochSecond
    println(s"fast: ${fast_time}")


//    before = new Date().toInstant
//    getAvgItemCountPerReturn_faster(ss, 1997)
//    getAvgItemCountPerReturn_faster(ss, 1995)
//    getAvgItemCountPerReturn_faster(ss, 1994)
//    after = new Date().toInstant
//    val faster_time = after.getEpochSecond - before.getEpochSecond
//    println(s"faster: ${faster_time}")

  }
}
