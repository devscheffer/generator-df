import generatorDf.{SingleColumnData, df_generate}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.funspec.AnyFunSpec

class TestGeneratorDf extends AnyFunSpec with SparkSessionTest {
  describe("Teste 1") {
    it("wip") {
      val base_dict = Seq(
        SingleColumnData("c1", IntegerType, Seq(0, 1)),
        SingleColumnData("c2", IntegerType, Seq(0, 1))
      )
      val df4 = df_generate(base_dict, spark)
      df4.show()
    }
  }

}
