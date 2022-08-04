package com.experian.latam.coe.utils

import com.experian.latam.coe.utils.GeneratorDf.{SingleColumnData, dfGenerate}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.FunSpec

class TestGeneratorDf extends FunSpec with SparkSessionTestWrapper {

  /** This creates a SparkSession, which will be used to operate on the
    * DataFrames that we create.
    */
  it("test generator") {

    val base_dict =
      Seq(
        SingleColumnData("c1", IntegerType, Seq(0, 1)),
        SingleColumnData("c2", IntegerType, Seq(0, 1))
      )
    dfGenerate(base_dict, spark)
      .show()
  }
}
