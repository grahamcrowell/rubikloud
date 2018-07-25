package com.rubikloud

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

/**
  * Data transfer objects for the TPH-C schema
  */
object dto {

  case class PART(P_PARTKEY: Long,
                  P_NAME: String,
                  P_MFGR: String,
                  P_BRAND: String,
                  P_TYPE: String,
                  P_SIZE: Int,
                  P_CONTAINER: String,
                  P_RETAILPRICE: Float,
                  P_COMMENT: String
                 )

  case class SUPPLIER(S_SUPPKEY: Long,
                      S_NAME: String,
                      S_ADDRESS: String,
                      S_NATIONKEY: Long,
                      S_PHONE: String,
                      S_ACCTBAL: Float,
                      S_COMMENT: String
                     )

  case class PARTSUPP(PS_PARTKEY: Long,
                      PS_SUPPKEY: Long,
                      PS_AVAILQTY: Int,
                      PS_SUPPLYCOST: Float,
                      PS_COMMENT: String
                     )

  case class CUSTOMER(C_CUSTKEY: Long,
                      C_NAME: String,
                      C_ADDRESS: String,
                      C_NATIONKEY: Long,
                      C_PHONE: String,
                      C_ACCTBAL: Float,
                      C_MKTSEGMENT: String,
                      C_COMMENT: String
                     )


  case class ORDERS(O_ORDERKEY: Long,
                    O_CUSTKEY: Long,
                    O_ORDERSTATUS: String,
                    O_TOTALPRICE: Float,
                    O_ORDERDATE: String,
                    O_ORDERPRIORITY: String,
                    O_CLERK: String,
                    O_SHIPPRIORITY: Int,
                    O_COMMENT: String
                   )

  case class LINEITEM(L_ORDERKEY: Long,
                      L_PARTKEY: Long,
                      L_SUPPKEY: Long,
                      L_LINENUMBER: Int,
                      L_QUANTITY: Float,
                      L_EXTENDEDPRICE: Float,
                      L_DISCOUNT: Float,
                      L_TAX: Float,
                      L_RETURNFLAG: String,
                      L_LINESTATUS: String,
                      L_SHIPDATE: String,
                      L_COMMITDATE: String,
                      L_RECEIPTDATE: String,
                      L_SHIPINSTRUCT: String,
                      L_SHIPMODE: String,
                      L_COMMENT: String
                     )

  case class NATION(N_NATIONKEY: Long,
                    N_NAME: String,
                    N_REGIONKEY: Long,
                    N_COMMENT: String
                   )

  case class REGION(R_REGIONKEY: Long,
                    R_NAME: String,
                    R_COMMENT: String
                   )

  val PART_schema = ScalaReflection.schemaFor[PART].dataType.asInstanceOf[StructType]
  val SUPPLIER_schema = ScalaReflection.schemaFor[SUPPLIER].dataType.asInstanceOf[StructType]
  val PARTSUPP_schema = ScalaReflection.schemaFor[PARTSUPP].dataType.asInstanceOf[StructType]
  val CUSTOMER_schema = ScalaReflection.schemaFor[CUSTOMER].dataType.asInstanceOf[StructType]
  val ORDERS_schema = ScalaReflection.schemaFor[ORDERS].dataType.asInstanceOf[StructType]
  val LINEITEM_schema = ScalaReflection.schemaFor[LINEITEM].dataType.asInstanceOf[StructType]
  val NATION_schema = ScalaReflection.schemaFor[NATION].dataType.asInstanceOf[StructType]
  val REGION_schema = ScalaReflection.schemaFor[REGION].dataType.asInstanceOf[StructType]


  /** Customized case class for partitioning question 5 (see [[question5.PartitionTable]]
    */

  case class LINEITEM_SHIPDATE(L_ORDERKEY: Long,
                               L_PARTKEY: Long,
                               L_SUPPKEY: Long,
                               L_LINENUMBER: Int,
                               L_QUANTITY: Float,
                               L_EXTENDEDPRICE: Float,
                               L_DISCOUNT: Float,
                               L_TAX: Float,
                               L_RETURNFLAG: String,
                               L_LINESTATUS: String,
                               L_SHIPDATE: String,
                               // break naming convention for demo purposes... make it obvious what I changed from default.
                               L_ship_year: Int,
                               L_ship_month: Int,
                               L_ship_day: Int,
                               L_COMMITDATE: String,
                               L_RECEIPTDATE: String,
                               L_SHIPINSTRUCT: String,
                               L_SHIPMODE: String,
                               L_COMMENT: String
                              )
  val LINEITEM_SHIPDATE_schema = ScalaReflection.schemaFor[LINEITEM_SHIPDATE].dataType.asInstanceOf[StructType]

}


