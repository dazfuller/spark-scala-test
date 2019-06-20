package com.github.dazfuller.databricks_lib

import com.holdenkarau.spark.testing.SharedSparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class UtilsTest extends FunSuite with SharedSparkContext {
  test("test initializing spark context") {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._

    val data = Seq(
      (1, 2, 3),
      (4, 5, 6)
    ).toDF("col1", "col2", "col3")

    assert(data.count() == 2)
  }
}
