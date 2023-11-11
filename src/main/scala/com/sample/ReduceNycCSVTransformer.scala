package com.sample

import io.smartdatalake.workflow.action.generic.customlogic.CustomGenericDfTransformer
import io.smartdatalake.workflow.action.spark.customlogic.{CustomDfTransformer, CustomDfsTransformer}
import io.smartdatalake.workflow.dataframe.{DataFrameFunctions, GenericDataFrame}
import org.apache.spark.sql.{DataFrame, SparkSession}

class ReduceNycCSVTransformer extends CustomDfTransformer { //  CustomGenericDfTransformer

  /*
  override def transform(session: SparkSession, options: Map[String, String], dfs: Map[String, DataFrame]): Map[String, DataFrame] = {
    import session.implicits._
    val df = dfs("ab-csv-org")
    df.select($"id".cast("int"), $"name")
    Map("ab-reduced-hsqldb" -> df)
  }*/
  /*
  override def transform(helper: DataFrameFunctions, options: Map[String, String], df: GenericDataFrame, dataObjectId: String): GenericDataFrame = {
    import session.implicits._
    df.select($"id".cast("int"), $"name")
  }
  */
  override def transform(session: SparkSession, options: Map[String,String], df: DataFrame, dataObjectId: String) : DataFrame = {
    import session.implicits._
    df.select($"id".cast("int"), $"name")
  }

}
