package com.github.dazfuller.databricks_lib

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

class Utils(spark: SparkSession = SparkSession.builder().getOrCreate()) {

  import spark.implicits._

  def latestRecords(df: DataFrame, idColumn: String, timestampColumn: String): DataFrame = {
    val recordWindow = Window.partitionBy(col(idColumn)).orderBy(col(timestampColumn).desc)

    val latest = df
      .withColumn("window_rank", row_number().over(recordWindow))
      .filter($"window_rank" === 1)
      .drop("window_rank")

    latest
  }
}
