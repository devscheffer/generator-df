package utils

import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object GeneratorDf extends SparkSessionWrapper {
  def dfGenerate(
      dataset: Seq[SingleColumnData],
      spark: SparkSession
  ): DataFrame = {
    dataset
      .map(x => listToDf(x, spark))
      .reduce((x, y) => x.crossJoin(y)))
      .coalesce(1)
  }

  def listToDf(dataset: SingleColumnData, spark: SparkSession): DataFrame = {
    val dataSchema = StructType(
      Array(
        StructField(dataset.columnName, dataset.columnType)
      )
    )
    val dataRows = spark.sparkContext.parallelize(dataset.columnDataTreated)
    spark.createDataFrame(dataRows, dataSchema)
  }

  case class SingleColumnData(
      columnName: String,
      columnType: DataType,
      columnData: Seq[Any]
  ) {
    val columnDataTreated: Seq[Row] = columnData.map(x => Row(x))
  }
}
