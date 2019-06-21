package com.github.dazfuller.databricks_lib

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Defines a set of utilities for working with [[org.apache.spark.sql.DataFrame]]
  *
  * @param spark The spark session to use, if not specified the current spark session is retrieved
  */
class Utils(spark: SparkSession = SparkSession.builder().getOrCreate()) {

  import spark.implicits._

  /**
    * Filters a data frame to return only the latest records
    *
    * @param df                The data frame to filter
    * @param idColumn          the name of the column containing the primary identifier (key) in the data frame
    * @param changeOrderColumn name of the column which identifies the date and time of the change
    */
  def latestRecords(df: DataFrame, idColumn: String, changeOrderColumn: String): DataFrame = {
    if (!df.columns.contains(idColumn)) {
      throw new IllegalArgumentException(s"Identifier column '$idColumn' not found in DataFrame")
    } else if (!df.columns.contains(changeOrderColumn)) {
      throw new IllegalArgumentException(s"Change order column '$changeOrderColumn' not found in DataFrame")
    }

    val recordWindow = Window.partitionBy(col(idColumn)).orderBy(col(changeOrderColumn).desc)

    val latest = df
      .withColumn("window_rank", row_number().over(recordWindow))
      .filter($"window_rank" === 1)
      .drop("window_rank")

    latest
  }
}
