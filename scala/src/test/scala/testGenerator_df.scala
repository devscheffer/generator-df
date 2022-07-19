import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import generator_df.{SingleColumnData, df_generate}

case object TestGenerator_df extends App with SparkSessionTest {

  val base_dict = Seq(
    SingleColumnData("c1", IntegerType, Seq(0, 1)),
    SingleColumnData("c2", IntegerType, Seq(0, 1)),
  )
  val df4 = df_generate(base_dict, spark)
  df4.show()
}