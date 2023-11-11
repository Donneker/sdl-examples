package com.sample

import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfTransformer, CustomDfsTransformer}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReduceNycCSVTransformer extends CustomDfsTransformer {

  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    import session.implicits._
    val df = dfs("ab-csv-org")
    df.select($"id".cast("int"), $"name")
    Map("ab-reduced-hsqldb" -> df)
  }
/*
  override def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
    import session.implicits._
    df.select($"id".cast("int"), $"name")
  }
 */
}
