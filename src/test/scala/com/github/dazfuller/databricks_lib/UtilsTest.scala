package com.github.dazfuller.databricks_lib

import java.sql.Timestamp

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.scalatest.{FunSuite, Matchers}

class UtilsTest extends FunSuite with SharedSparkContext with Matchers {
  // Disable INFO logging to de-clutter output
  Logger.getRootLogger.setLevel(Level.WARN)

  val cols = List("id", "pk_field", "value", "last_modified")

  test("test initializing spark context") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1, 2, 3),
      (4, 5, 6)
    ).toDF("col1", "col2", "col3")

    data.count should be(2)
  }

  test("Only latest records should be left") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val startDate = new DateTime(2019, 2, 13, 10, 10)

    val df = Seq(
      (1, 1L, 1.0, new Timestamp(startDate.getMillis)),
      (2, 1L, 2.0, new Timestamp(startDate.plusDays(1).getMillis)),
      (3, 1L, 3.0, new Timestamp(startDate.plusDays(2).getMillis)),
      (4, 1L, 4.0, new Timestamp(startDate.minusDays(3).getMillis))
    ).toDF(this.cols: _*)

    val utils = new Utils(spark)
    val latest = utils.latestRecords(df, "pk_field", "last_modified")

    latest.count should be(1)
    latest.head.getInt(0) should be(3)
  }

  test("Utils should use default spark value") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val startDate = new DateTime(2019, 2, 13, 10, 10)

    val df = Seq(
      (1, 1L, 1.0, new Timestamp(startDate.getMillis)),
      (2, 1L, 2.0, new Timestamp(startDate.plusDays(1).getMillis))
    ).toDF(this.cols: _*)

    val utils = new Utils()
    val latest = utils.latestRecords(df, "pk_field", "last_modified")

    latest.count should be(1)
  }

  test("Providing an invalid identifier column throws an error") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, 2, 3)
    ).toDF("a", "b", "c")

    val utils = new Utils()

    val thrown = the[IllegalArgumentException] thrownBy utils.latestRecords(df, "d", "c")
    thrown.getMessage should startWith("Identifier column 'd'")
  }

  test("Providing an invalid change order column throws an error") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val df = Seq(
      (1, 2, 3)
    ).toDF("a", "b", "c")

    val utils = new Utils()

    val thrown = the[IllegalArgumentException] thrownBy utils.latestRecords(df, "c", "Timestamp")
    thrown.getMessage should startWith("Change order column 'Timestamp'")
  }
}
