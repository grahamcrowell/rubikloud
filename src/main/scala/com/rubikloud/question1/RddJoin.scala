package com.rubikloud.question1

import com.rubikloud.dto
import com.rubikloud.path_prefix
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Question 1 Part c)
  * Pick any two datasets and join them using Spark's API, demonstrate join using b) RDD
  *
  * uses key-value pattern to join
  */
object RddJoin {
  /** Note: this approach doesn't consider types of columns (numeric vs string etc)
    * [[rddOfCaseClass]] considers type.
    */
  def rddOfStringArray(sc: SparkContext): Unit = {
    // set column index of foreign key to NATION
    val customer_nation_fk_column_idx = dto.CUSTOMER_schema.fieldIndex("C_NATIONKEY")

    // read delimited file as an RDD
    val customer_rdd = sc.textFile(path = s"${path_prefix}DATA/customer.tbl")
      // split into columns
      .map(
      // double escape to override regex OR symbol as pipe character delimiter
      (data_line: String) => data_line.split("\\|")
    )
      // convert into key-value pairs, where key is column to be joined
      .map(
      (row: Array[String]) => Tuple2(row(customer_nation_fk_column_idx), row)
    )

    // sanity check
    assert(customer_rdd.isInstanceOf[RDD[(String, Array[String])]])

    // set column index of NATION pk
    val nation_pk_column_idx = dto.NATION_schema.fieldIndex("N_NATIONKEY")

    // read delimited file as an RDD
    val nation_rdd = sc.textFile(path = s"${path_prefix}DATA/nation.tbl")
      // split into columns
      .map(
      // double escape to override regex OR symbol as pipe character delimiter
      (data_line: String) => data_line.split("\\|")
    )
      // convert into key-value pairs, where key is column to be joined
      .map(
      (row: Array[String]) => Tuple2(row(nation_pk_column_idx), row)
    )

    // sanity check
    assert(nation_rdd.isInstanceOf[RDD[(String, Array[String])]])

    /**
      * RDD key-value join
      */
    // each element of joined is a nested pair (K, (A,B)) where K is key, A is nation, B is customer
    val joined: RDD[(String, (Array[String], Array[String]))] = nation_rdd.join(customer_rdd)
    // demo 1st record of join
    val joinedRow = joined.first()
    // for demo purposes
    val key: String = joinedRow._1
    val nation_customer_pair: (Array[String], Array[String]) = joinedRow._2
    val nation_row: Array[String] = nation_customer_pair._1
    val customer_row: Array[String] = nation_customer_pair._2
    println(key)
    nation_row.foreach(println)
    customer_row.foreach(println)

    // sanity check
    assert(joined.isInstanceOf[RDD[(String, (Array[String], Array[String]))]])

  }

  /** Same as [[rddOfStringArray]] except with case classes.
    * This approach is requires more CPU than [[rddOfStringArray]] because of casting required to initialize case classes
    */
  def rddOfCaseClass(sc: SparkContext): Unit = {
    // set column index of foreign key to NATION
    val customer_nation_fk_column_idx = dto.CUSTOMER_schema.fieldIndex("C_NATIONKEY")

    // read delimited file as an RDD
    val customer_rdd = sc.textFile(path = s"${path_prefix}DATA/customer.tbl")
      // split into columns
      .map(
      // double escape to override regex OR symbol as pipe character delimiter
      (data_line: String) => data_line.split("\\|")
    ).map(
      // convert each row to an instance of CUSTOMER
      (row: Array[String]) => {
        dto.CUSTOMER(
          row(0).toLong,
          row(1),
          row(2),
          row(3).toLong,
          row(4),
          row(5).toFloat,
          row(6),
          row(7)
        )
      }
    )
      // convert into key-value pairs, where key is column to be joined
      .map(
      (customer: dto.CUSTOMER) => Tuple2(customer.C_NATIONKEY, customer)
    )

    // sanity check
    assert(customer_rdd.isInstanceOf[RDD[(Long, dto.CUSTOMER)]])

    // set column index of NATION pk
    val nation_pk_column_idx = dto.NATION_schema.fieldIndex("N_NATIONKEY")

    // read delimited file as an RDD
    val nation_rdd = sc.textFile(path = s"${path_prefix}DATA/nation.tbl")
      // split into columns
      .map(
      // double escape to override regex OR symbol as pipe character delimiter
      (data_line: String) => data_line.split("\\|")
    ).map(
      // convert each row to an instance of NATION
      (row: Array[String]) => {
        dto.NATION(
          row(0).toLong,
          row(1),
          row(2).toLong,
          row(3)
        )
      }
    )
      // convert into key-value pairs, where key is column to be joined
      .map(
      (nation: dto.NATION) => Tuple2(nation.N_NATIONKEY, nation)
    )

    // sanity check
    assert(nation_rdd.isInstanceOf[RDD[(Long, dto.NATION)]])


    /**
      * RDD key-value join
      */
    // each element of joined is a nested pair (K, (A,B)) where K is key, A is nation, B is customer
    val joined: RDD[(Long, (dto.NATION, dto.CUSTOMER))] = nation_rdd.join(customer_rdd)
    // demo 1st record of join
    val joinedRow: (Long, (dto.NATION, dto.CUSTOMER)) = joined.first()
    // for demo purposes
    val key: Long = joinedRow._1
    val nation_customer_pair: (dto.NATION, dto.CUSTOMER) = joinedRow._2
    val nation: dto.NATION = nation_customer_pair._1
    val customer: dto.CUSTOMER = nation_customer_pair._2
    println(key)
    println(nation)
    println(customer)


    // sanity check
    assert(joined.isInstanceOf[RDD[(String, (dto.NATION, dto.CUSTOMER))]])
  }

  def main(args: Array[String]): Unit = {

    val appName = "Question1_PartB_DataSetJoin"

    lazy val conf = new SparkConf()
      .setAppName(appName)
    lazy val sc = SparkContext.getOrCreate(conf)

    rddOfStringArray(sc)
    rddOfCaseClass(sc)


  }
}
