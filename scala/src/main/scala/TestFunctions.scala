package com.experian.latam.coe.utils

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}

object TestFunctions {

  def checkCondition(
      inputDf: DataFrame,
      testCondition: Seq[Column]
  ): Boolean = {
    val dfTested: DataFrame = {
      testCondition.zipWithIndex.foldLeft(inputDf)((df, condition) => {
        df.withColumn(s"c${condition._2}", condition._1)
      })
    }

    val dataIndexEnd: Integer = inputDf.columns.length
    val conditionSize: Integer = testCondition.length
    val dfTestedV2: DataFrame =
      dfTested
        .select(
          dfTested.columns
            .slice(dataIndexEnd, dataIndexEnd + conditionSize)
            .map(m => col(m)): _*
        )

    val dfTestedV3: Boolean =
      dfTestedV2.columns
        .map(x => {
          dfTestedV2.select(x).filter(!col(x)).isEmpty
        })
        .forall(x => x)

    List(!inputDf.isEmpty, dfTestedV3).forall(x => x)
  }
}
