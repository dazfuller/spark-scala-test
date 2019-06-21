package com.github.dazfuller.databricks_lib

import java.sql.Timestamp

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.scalatest.FunSuite

class UtilsTest extends FunSuite with SharedSparkContext {
  val cols = List("id", "pk_field", "value", "last_modified")

  test("test initializing spark context") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1, 2, 3),
      (4, 5, 6)
    ).toDF("col1", "col2", "col3")

    assert(data.count() == 2)
  }

  test("Only latest records should be left") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val startDate = new DateTime(2019, 2, 13, 10, 10)

    val df = Seq(
      (1, 1L, 1.0, new Timestamp(startDate.getMillis)),
      (2, 1L, 2.0, new Timestamp(startDate.plusDays(1).getMillis)),
      (3, 1L, 3.0, new Timestamp(startDate.plusDays(2).getMillis)),
      (4, 1L, 4.0, new Timestamp(startDate.plusDays(3).getMillis))
    ).toDF(this.cols: _*)

    val utils = new Utils(spark)
    val latest = utils.latestRecords(df, "pk_field", "last_modified")

    assert(latest.count == 1)
    assert(latest.head.getInt(0) == 4)
  }
}
